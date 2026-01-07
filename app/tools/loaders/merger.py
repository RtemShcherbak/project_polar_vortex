import xarray as xr

from app.tools.configs import Logger



class Merger:
    def __init__(self):
        self.logger = Logger(name='data_loading_logger')

    ## ------------------------------------------------------------------------
    ## Private
    ## ------------------------------------------------------------------------
    @staticmethod
    def __drop_run_coords(ds: xr.Dataset) -> xr.Dataset:
        return ds.reset_coords([c for c in ["time", "step"] if c in ds.coords], drop=True)

    @staticmethod
    def __norm_grid(ds: xr.Dataset) -> xr.Dataset:
        if ds.latitude.values[0] > ds.latitude.values[-1]:
            ds = ds.sortby("latitude")
        # на всякий случай, если где-то долгота идёт не так:
        if ds.longitude.values[0] > ds.longitude.values[-1]:
            ds = ds.sortby("longitude")
        return ds


    ## ------------------------------------------------------------------------
    ## Public
    ## ------------------------------------------------------------------------
    def merge_datasets(
            self,
            era_fact_xds: xr.Dataset,
            gfs_fact_xds: xr.Dataset,
            gfs_forecast_xds: xr.Dataset,
    ):
        self.logger.info("[MERGER] start merge data")
        ## ---------------------------------------------------------------------
        ## GFS xds
        ## ---------------------------------------------------------------------
        self.logger.debug("[MERGER] start prepare gfs")
        gfs_fact_xds = self.__norm_grid(
            self.__drop_run_coords(gfs_fact_xds)
        )
        gfs_forecast_xds = self.__norm_grid(
            self.__drop_run_coords(gfs_forecast_xds)
        )
        extra_forecast = gfs_forecast_xds.sel(
            valid_time=~gfs_forecast_xds.valid_time.isin(gfs_fact_xds.valid_time)
        )
        gfs_data = xr.concat(
            [gfs_fact_xds, extra_forecast],
            dim="valid_time",
            coords="minimal",
            join="exact",
        ).sortby("valid_time")
        gfs_data = (
            gfs_data
            .rename({"isobaricInhPa": "pressure_level"})
            .resample(valid_time="1D").mean()
        )
        self.logger.debug("[MERGER] end prepare gfs")
        ## ---------------------------------------------------------------------
        ## ERA xds
        ## ---------------------------------------------------------------------
        self.logger.debug("[MERGER] start prepare era")
        era_data = era_fact_xds.load().reset_coords(
            ["number", "expver"], drop=True
        )
        self.logger.debug("[MERGER] end prepare era")
        ## ---------------------------------------------------------------------
        ## Merge xds
        ## ---------------------------------------------------------------------
            # 1) привести lon GFS к диапазону ERA, (0..360 -> -180..180)
        gfs_on_era_lon = gfs_data.assign_coords(
            longitude=(((gfs_data.longitude + 180) % 360) - 180)
        ).sortby("longitude")
            # 2) взять только те точки, которые есть в ERA
        gfs_sub = gfs_on_era_lon.sel(
            latitude=era_data.latitude,
            longitude=era_data.longitude,
            pressure_level=era_data.pressure_level,
        )
            # 3) ERA берём как есть, только сортируем по времени 
        era_s = era_data.sortby("valid_time")
        gfs_s = gfs_sub.sortby("valid_time")
            # 4) combine_first: берёт значения из объекта слева, 
            # а где там NaN/нет данных — добирает из правого
        ds = era_s.combine_first(gfs_s)
        self.logger.info("[MERGER] end merge data")
        return ds
