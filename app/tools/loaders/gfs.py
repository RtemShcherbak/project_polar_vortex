import pandas as pd
import numpy as np
import xarray as xr
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from app.tools.configs import GFS_const, Constant, Logger
from app.tools.loaders.exceptions import (
    TemporaryDataUnavailable, IncompleteDataError, 
    DataValidationError, FatalPipelineError
)


class GFS_loader:
    def __init__(
            self
        ):
        self.const = Constant()
        self.gfs_const = GFS_const()
        self.logger = Logger(name='data_loading_logger')
        ## ------------------------------------------------------------------------


    ## ------------------------------------------------------------------------
    ## Private
    ## ------------------------------------------------------------------------
    def __daterange(
            self,
            d0: datetime, 
            d1: datetime
        ):
        """inclusive dates at 00:00"""
        d = d0
        while d <= d1:
            yield d
            d += timedelta(days=1)


    def __build_url(
            self,
            run_date: datetime, 
            cycle_hh: int, 
            fhr: int,
        ) -> str:
        ymd = run_date.strftime("%Y%m%d")
        file_ = f"gfs.t{cycle_hh:02d}z.pgrb2.0p25.f{fhr:03d}"
        ## IMPORTANT: 
        ## dir must be urlencoded with leading /gfs.YYYYMMDD/HH/atmos
        dir_ = f"/gfs.{ymd}/{cycle_hh:02d}/atmos"
        params = dict(self.gfs_const.request)
        params.update(
            {
                "file": file_,
                ## NOMADS uses "dir" as a path
                "dir": dir_,
            }
        )
        return self.gfs_const.base_url + "?" + urlencode(params)


    def __download(
        self,
        url: str,
        out_path: Path,
        timeout: int = 20,
    ):
        out_path = Path(out_path)
        tmp_path = out_path.with_suffix(out_path.suffix + ".part")
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            # NOMADS иногда возвращает текст с ошибкой
            ctype = r.headers.get("Content-Type", "")
            if "text" in ctype.lower():
                text = r.text[:500]
                raise RuntimeError(
                    f"Server returned text response (likely error): {text}"
                )
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            with tmp_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
        # атомарная замена
        tmp_path.replace(out_path)


    def __try_latest_cycle(
        self,
        run_date: datetime,
        preferred_cycles=(18, 12, 6, 0),
        test_fhr=0,
    ) -> int:
        """
        Пытаемся найти самый свежий доступный цикл GFS.
        """
        self.logger.loading(
            f"Searching latest GFS cycle for {run_date.date()} "
        )

        sess = requests.Session()

        for hh in preferred_cycles:
            url = self.__build_url(run_date, hh, test_fhr)

            try:
                # сначала пробуем HEAD
                head = sess.head(url, timeout=20, allow_redirects=True)
                if 200 <= head.status_code < 300:
                    self.logger.loading(
                        f"Found available GFS cycle: {hh:02d}Z (HEAD OK)"
                    )
                    return hh

                if head.status_code in (400, 404, 500):
                    self.logger.debug(
                        f"GFS cycle {hh:02d}Z not available (HEAD {head.status_code})"
                    )
                    continue

                # fallback: GET с Range
                g = sess.get(
                    url,
                    headers={"Range": "bytes=0-1024"},
                    timeout=30,
                    stream=True,
                )
                if 200 <= g.status_code < 300 or g.status_code == 206:
                    self.logger.loading(
                        f"Found available GFS cycle: {hh:02d}Z (GET Range OK)"
                    )
                    return hh

                self.logger.debug(
                    f"GFS cycle {hh:02d}Z not available (GET {g.status_code})"
                )

            except Exception as e:
                # это не фатально — логируем и пробуем следующий цикл
                self.logger.debug(
                    f"Error probing GFS cycle {hh:02d}Z: {type(e).__name__}: {e}"
                )
                continue

        # если дошли сюда — ничего не нашли
        self.logger.loading_error(
            f"Could not find available GFS cycle for {run_date.date()} "
            f"among {preferred_cycles}"
        )
        # raise RuntimeError(
        #     f"Could not find available cycle for {run_date.date()} "
        #     f"among {preferred_cycles}"
        # )
        raise TemporaryDataUnavailable(
            f"GFS cycle not yet available for {run_date.date()}"
        )

    
    def __merge_tmpfs(
        self,
        grib_dir: Path,
        out_nc: Path,
    ):
        grib_dir = Path(grib_dir)
        out_nc = Path(out_nc)

        dsets = []

        try:
            for p in sorted(grib_dir.glob("*.grib2")):
                ds = xr.open_dataset(p, engine="cfgrib")
                # valid_time = time + step (стандартный случай GFS)
                if "step" in ds.coords:
                    vt = ds["time"] + ds["step"]
                else:
                    vt = ds["time"]
                # гарантируем размерность valid_time=1
                if "valid_time" not in ds.dims:
                    ds = ds.assign_coords(valid_time=vt)
                    ds = ds.expand_dims(
                        valid_time=[np.array(vt.values).item()]
                    )
                # step больше не нужен
                if "step" in ds.variables and "step" not in ds.dims:
                    ds = ds.drop_vars("step")
                dsets.append(ds)
            if not dsets:
                raise IncompleteDataError(
                    f"No GFS GRIB files found in {grib_dir}"
                )

            ds_all = xr.concat(
                dsets,
                dim="valid_time",
                data_vars="minimal",
                coords="minimal",
                compat="override",
            )

            # приведение и сортировка времени
            ds_all = ds_all.assign_coords(
                valid_time=ds_all.valid_time.astype("datetime64[ns]")
            )
            ds_all = ds_all.sortby("valid_time")

            out_nc.parent.mkdir(parents=True, exist_ok=True)
            ds_all.to_netcdf(out_nc, engine="netcdf4")

            return ds_all
        
        except IncompleteDataError:
            raise

        except Exception as e:
            raise DataValidationError(
                f"GFS merge failed for directory {grib_dir}"
            ) from e


    def __remove_tmpfs(self, grib_dir):
        if self.gfs_const.cleanup_tmpfs:
            for p in grib_dir.glob("*.grib2"):
                p.unlink()
            for p in grib_dir.glob("*.idx"):
                p.unlink()


    ## ------------------------------------------------------------------------
    ## Public
    ## ------------------------------------------------------------------------
    def load_fact(self):
        # ----------------------------------------
        # FACT: daily analyses f000
        # ----------------------------------------
        fact_start = str(
            (self.const.today - pd.Timedelta(days=self.gfs_const.era_reanalisys_delay)).date()
        )
        init = str(self.const.today.date())

        fact_start = datetime.strptime(fact_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        init_date = (
            datetime.now(timezone.utc).date()
            if init is None
            else datetime.strptime(init, "%Y-%m-%d").date()
        )
        init_dt = datetime.combine(init_date, datetime.min.time(), tzinfo=timezone.utc)

        fact_cycle = int(self.gfs_const.fact_cycle)
        fact_end = init_dt  # факт качаем до дня init включительно (analysis)

        self.logger.loading(
            f"[FACT_GFS] {fact_start.date()} .. {fact_end.date()} "
            f"cycle={fact_cycle:02d}Z f000"
        )

        for d in self.__daterange(fact_start, fact_end):
            ymd = d.strftime("%Y%m%d")
            out_dir = self.const.fact_dir / f"gfs_tmp_files"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"gfs_{ymd}_{fact_cycle:02d}z_f000.grib2"

            if out_file.is_file():
                self.logger.loading(f"[FACT_GFS] skip exists: {out_file}")
                continue

            url = self.__build_url(
                run_date=d,
                cycle_hh=fact_cycle,
                fhr=0,
            )

            try:
                self.logger.loading(
                    f"[FACT_GFS] download: {ymd} {fact_cycle:02d}Z f000"
                )
                self.__download(url, out_file)

            except Exception as e:
                self.logger.loading_error(
                    f"[FACT_GFS] FAILED {ymd}: {type(e).__name__}: {e}"
                )
                raise IncompleteDataError(
                    f"GFS FACT download incomplete, failed at {ymd}"
                ) from e

        self.logger.loading("[FACT_GFS] end loading")


    def load_forecast(self,):
        fact_start = str(
            (self.const.today - pd.Timedelta(days=self.gfs_const.era_reanalisys_delay)).date()
        )
        init = str(self.const.today.date())
        fact_start = datetime.strptime(fact_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        init_date = (
            datetime.now(timezone.utc).date()
            if init is None
            else datetime.strptime(init, "%Y-%m-%d").date()
        )
        init_dt = datetime.combine(init_date, datetime.min.time(), tzinfo=timezone.utc)
        # ----------------------------------------
        # FORECAST: from init run, f000..fN step
        # ----------------------------------------
        fcst_cycle = (
            self.__try_latest_cycle(init_dt) if self.gfs_const.forecast_cycle == "auto" \
                else int(self.gfs_const.forecast_cycle)
        )
        max_hour = self.gfs_const.forecast_days * 24
        step = self.gfs_const.forecast_step_hours
        self.logger.loading(
            f"[FORECAST_GFS] init={init_dt.date()} cycle={fcst_cycle:02d}Z f000..f{max_hour:03d} step={step}h"
        )
        run_dir = self.const.forecast_dir / f"gfs_tmp_files"
        run_dir.mkdir(parents=True, exist_ok=True)

        for fhr in range(0, max_hour + 1, step):
            out_file = run_dir / f"gfs_{init_dt.strftime('%Y%m%d')}_{fcst_cycle:02d}z_f{fhr:03d}.grib2"
            if out_file.exists():
                self.logger.loading(f"[FORECAST_GFS]  skip exists: {out_file}")
                continue

            url = self.__build_url(
                run_date=init_dt, 
                cycle_hh=fcst_cycle, 
                fhr=fhr,
            )
            try:
                self.logger.loading(f"[FORECAST_GFS]  download: f{fhr:03d}-{fhr//24}:{fhr%24}")
                self.__download(url, str(out_file))
            except Exception as e:
                self.logger.loading_error(f"  FAILED f{fhr:03d}: {e}")#, file=sys.stderr)
                raise IncompleteDataError(
                    f"GFS FORECAST incomplete, failed at f{fhr:03d}"
                ) from e

        self.logger.loading("[FORECAST_GFS] end loading")
    

    def merge_fact_tmpfs(self):
        try: 
            xds_filename = self.const.fact_dir / f"gfs_data.nc"
            ds_all = self.__merge_tmpfs(
                grib_dir=self.const.fact_dir / "gfs_tmp_files",
                out_nc=xds_filename
            )
            self.logger.loading(
                f"[FACT_GFS] merge gfs fact data: {xds_filename}"
            )
        except Exception as e:
            self.logger.loading_error(
                f"[FACT_GFS] FAILED merge gfs fact data: {type(e).__name__}: {e}"
            )
            
        return ds_all


    def merge_forecast_tmpfs(self):
        try: 
            xds_filename = (self.const.forecast_dir / f"gfs_data.nc")
            ds_all = self.__merge_tmpfs(
                grib_dir=self.const.forecast_dir / "gfs_tmp_files",
                out_nc=xds_filename
            )
            self.logger.loading(
                f"[FORECAST_GFS] merge gfs fact data: {xds_filename}"
            )
        except Exception as e:
            self.logger.loading_error(
                f"[FORECAST_GFS] FAILED merge gfs fact data: {type(e).__name__}: {e}"
            )
        return ds_all


    def remove_fact_tmpfs(self):
        self.logger.loading(f"[FACT_GFS] remove old tmp files")
        self.__remove_tmpfs(self.const.fact_dir / f"gfs_tmp_files")

    def remove_forecast_tmpfs(self):
        self.logger.loading(f"[FORECAST_GFS] remove old tmp files")
        self.__remove_tmpfs(self.const.forecast_dir / f"gfs_tmp_files")

