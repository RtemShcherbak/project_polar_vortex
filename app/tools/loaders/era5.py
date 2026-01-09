import cdsapi
import logging
import pandas as pd
import xarray as xr

from app.tools.configs import ERA_const, Constant, Logger
from app.tools.loaders.exceptions import (
    TemporaryDataUnavailable, IncompleteDataError, 
    DataValidationError, FatalPipelineError
)


class ERA5_loader:
    def __init__(
            self
    ):
        self.const = Constant()
        self.era_const = ERA_const()
        self.logger = Logger(name='data_loading_logger')
        ## ------------------------------------------------------------------------
        self.era_filename = lambda year: \
            f"era5_{self.const.today.date().strftime('%Y_%m_%d')}_polar_vortex_{year}.nc"


    ## ------------------------------------------------------------------------
    ## Private
    ## ------------------------------------------------------------------------
    def __specify_dates(self):
        era_dates = {}
        dates_range = pd.date_range(
            self.const.start_date, 
            self.const.today - pd.Timedelta(
                days=self.era_const.era_reanalisys_delay
            )
        )
        for pd_date in dates_range:
            year = pd_date.year
            month = pd_date.month
            if year not in era_dates.keys():
                era_dates[year] = [month]
            else:
                if month not in era_dates[year]:
                    era_dates[year].append(month)
        # for k, v in era_dates.items():
        #     era_dates[k] = set(v)
        return era_dates
    

    def __client(self):
        ## 1. First log - guarant init logger
        self.logger.loading("[FACT_ERA] Initializing CDS API client")
        ## 2. Add cdsapi lods to handlers self.logger
        cds_logger = logging.getLogger("cdsapi")
        cds_logger.setLevel(logging.INFO)
        cds_logger.propagate = False
        for h in self.logger._logger.handlers:
            cds_logger.addHandler(h)
        ## 3. Client
        try: 
            client = cdsapi.Client(
                url=self.era_const.url,
                key=self.era_const.key,
                verify=self.era_const.verify,
                quiet=self.era_const.quiet,
                timeout=self.era_const.timeout,
                retry_max=self.era_const.retry_max,
                sleep_max=self.era_const.sleep_max,
            )
            self.logger.loading("[FACT_ERA] Connected to CDS API OK")
            return client
        
        except Exception as e:
            raise TemporaryDataUnavailable(
                "CDS API is not available"
            ) from e


    ## ------------------------------------------------------------------------
    ## Public
    ## ------------------------------------------------------------------------
    def load(self):
        client = None
        dates_range = self.__specify_dates()
        self.logger.debug(f"[{type(self).__name__}] - load :: {dates_range}")
        ## ------------------- Loading ------------------- ##
        target_dir = self.const.fact_dir / "era_tmp_files"
        target_dir.mkdir(parents=True, exist_ok=True)
        for year, months in dates_range.items():
            target = target_dir / self.era_filename(year)
            if target.exists():
                self.logger.loading(f"[FACT_ERA] Already downloaded: {target}")
            else:
                if client is None:
                    client = self.__client()
                request = dict(self.era_const.request)
                request.update(
                    {
                        "year": str(year),
                        "month": months,
                    }
                )
                self.logger.loading(f"[FACT_ERA] Start downloading: {target}")
                try:
                    client.retrieve(
                        self.era_const.dataset,
                        request,
                        target,
                    )
                except Exception as e:
                    self.logger.loading_error(
                        f"[FACT_ERA] FAILED downloading {target} | {type(e).__name__}: {e}"
                    )
                    raise IncompleteDataError(
                        f"ERA5 download incomplete, failed for year={year}"
                    ) from e
                else:
                    self.logger.loading(f"[FACT_ERA] Finished downloading: {target}")


    def merge(self):
        era_nc_files = [
            self.const.fact_dir / "era_tmp_files" / self.era_filename(year)
            for year in self.__specify_dates().keys()
        ]
        missing = [f for f in era_nc_files if not f.exists()]
        if missing:
            raise DataValidationError(
                f"Missing ERA files before merge: {missing}"
            )

        target = (
            self.const.fact_dir /
            f"era_data_{self.const.today.date().strftime('%Y_%m_%d')}.nc"
        )
        self.logger.loading(
            f"[FACT_ERA] Opening {len(era_nc_files)} ERA NetCDF files"
        )

        try:
            era_data = xr.open_mfdataset(era_nc_files).load()
            era_data.to_netcdf(target)
        except Exception as e:
            self.logger.loading_error(
                f"[FACT_ERA] FAILED building ERA dataset | {type(e).__name__}: {e}"
            )
            raise DataValidationError(
                "ERA5 merge failed"
            ) from e
        else:
            self.logger.loading(
                f"[FACT_ERA] ERA dataset written to {target}"
            )
        return era_data


    def remove_tmpfs(self):
        """
        Полный cleanup временных ERA файлов.
        """
        tmp_dir = self.const.fact_dir / "era_tmp_files"

        if not tmp_dir.exists():
            return

        if self.era_const.cleanup_tmpfs:
            for f in tmp_dir.glob("*.nc"):
                try:
                    f.unlink()
                    self.logger.loading(
                        f"[FACT_ERA] Removed temporary file: {f}"
                    )
                except Exception as e:
                    self.logger.loading_error(
                        f"[FACT_ERA] FAILED removing {f} | {type(e).__name__}: {e}"
                    )

        
