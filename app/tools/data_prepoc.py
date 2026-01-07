import xarray as xr
import pandas as pd
import numpy as np
from functools import wraps

from configs import Logger, Constant


## ------------------------------------------------------------------------
## Supply decorator
## ------------------------------------------------------------------------
def log_fail(action: str):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                self.logger.info(
                    f"FAILED {action} | {type(e).__name__}: {e}"
                )
                raise
        return wrapper
    return decorator



class Vars:
    def __init__(self):
        self.logger = Logger(name='data_loading_logger')
        self.const = Constant()


    ## ------------------------------------------------------------------------
    ## Private
    ## ------------------------------------------------------------------------


    ## ------------------------------------------------------------------------
    ## Public
    ## ------------------------------------------------------------------------
    @log_fail("computing U(60N, 10 hPa)")
    def get_u60_df(self, ds: xr.Dataset) -> pd.DataFrame:
        self.logger.debug(
            "Computing U(60N, 10 hPa) zonal mean"
        )

        u_df = (
            ds.u
            .sel(latitude=60.0, pressure_level=10.0)
            .mean("longitude")
            .to_dataframe()[["u"]]
        )

        self.logger.debug(
            f"U60 dataframe built successfully, shape={u_df.shape}"
        )
        return u_df


    @log_fail("computing T polar cap(lats[90, 60], 10 hPa)")
    def get_t_df(self, ds: xr.Dataset) -> pd.DataFrame:
        self.logger.debug(
            "Computing T polar cap(lats[90, 60], 10 hPa) mean"
        )

        T_cap = ds["t"].sel(latitude=slice(90, 60), pressure_level=10.0)
        # веса по площади (cos(lat))
        w = np.cos(np.deg2rad(T_cap["latitude"]))
        w = xr.DataArray(w, coords={"latitude": T_cap["latitude"]}, dims=["latitude"])
        # area-weighted mean по широте и долготе
        T_cap_mean = T_cap.weighted(w).mean(("latitude", "longitude"))
        t_df = (
            T_cap_mean 
            .to_dataframe()[['t']]
            .reset_index()
            .rename(columns={'valid_time': 'date'})
            # .drop(columns=['pressure_level'])
            # .to_csv(ROOT_DATA / "hf_daily.csv")
        )
        t_df['date'] = pd.to_datetime(t_df['date'])
        t_df['t_C'] = t_df['t'] - 273.15
        t_df.set_index('date', inplace=True)

        self.logger.debug(
            f"Temp dataframe built successfully, shape={t_df.shape}"
        )
        return t_df
    

    @log_fail("computing eddy_hf_40_dataframe")
    def get_eddy_hf_df(
        self,
        ds: xr.Dataset,
        level: float = 100.0,
        lat_range: tuple = (45.0, 75.0),
        window_days: int = 40,
        drop_feb29: bool = True,
        time_dim_preference: tuple = ("time", "valid_time"),
        lon_dim: str = "longitude",
        lat_dim: str = "latitude",
        p_dim: str = "pressure_level",
        v_name: str = "v",
        t_name: str = "t",
    ) -> pd.DataFrame:
        """
        Returns: pandas.DataFrame with columns:
        - date (datetime64[ns])
        - eddy_hf_40 (float)  # 40-day mean heat-flux anomaly at 100 hPa, 45–75N
        """
        # 1) Resolve time coordinate name
        time_dim = None
        for cand in time_dim_preference:
            if cand in ds.coords or cand in ds.dims:
                time_dim = cand
                break
        if time_dim is None:
            # fallback: find first datetime-like coordinate
            for k, v in ds.coords.items():
                if np.issubdtype(v.dtype, np.datetime64):
                    time_dim = k
                    break
        if time_dim is None:
            raise ValueError("Could not find a datetime time coordinate (e.g., 'time' or 'valid_time').")
        # 2) Select level and latitude band
        lat_min, lat_max = min(lat_range), max(lat_range)
            # ERA-style latitude often descending; slice must follow actual ordering
        lat_vals = ds[lat_dim].values
        descending = lat_vals[0] > lat_vals[-1]
        lat_slice = slice(lat_max, lat_min) if descending else slice(lat_min, lat_max)
        ds_sel = (
            ds
            .sel({p_dim: float(level)})
            .sel({lat_dim: lat_slice})
        )
        # 3) Compute daily eddy heat flux HF(t) = < over lat band [ mean_lon( (v-vbar)*(T-Tbar) ) ] >
        v = ds_sel[v_name]
        T = ds_sel[t_name]
        v_bar = v.mean(lon_dim)
        T_bar = T.mean(lon_dim)
        v_prime = v - v_bar
        T_prime = T - T_bar
        vT_eddy = (v_prime * T_prime).mean(lon_dim)  # (time, lat)
            # cosine-lat weights for averaging over latitude
        weights = np.cos(np.deg2rad(vT_eddy[lat_dim]))
        HF = vT_eddy.weighted(weights).mean(lat_dim)  # (time,)
            # ensure time coord name is 'time' for convenience below
        HF = HF.rename({time_dim: "time"}) if time_dim != "time" else HF
            # optional: drop Feb 29 to make day-of-year climatology cleaner
        if drop_feb29:
            HF = HF.sel(time=~((HF.time.dt.month == 2) & (HF.time.dt.day == 29)))
        df_hf = HF.to_dataframe(name='hf')[['hf']]
        # 5) Polvani order:
        #    (a) 40-day trailing mean FIRST
        df_hf[f'hf_{window_days}'] = df_hf['hf'].rolling(window_days).mean()
        #    (b) climatology by day-of-year on HF40
        hf_daily = pd.read_csv(self.const.hf_daily_csv_path)
        hf_daily['date'] = pd.to_datetime(hf_daily['date'])
        hf_daily.drop(columns=['Unnamed: 0'], inplace=True)
        hf_daily.set_index('date', inplace= True)
        hf_daily[f'hf_{window_days}'] = hf_daily.rolling(window_days).mean()
        hf_daily['doy'] = hf_daily.index.day_of_year
        hf_daily_clim = (
            hf_daily
            .groupby('doy')
            .agg(mean_hf=(f'hf_{window_days}', "mean"))
        )
        hf_daily = (
            hf_daily
            .reset_index()
            .merge(hf_daily_clim, how='inner', on='doy')
            .set_index('date')
            .dropna()
        )
        mean_hf = hf_daily_clim.copy()
        hf_daily[f'hf_{window_days}_anomaly'] = hf_daily[f'hf_{window_days}'] - hf_daily['mean_hf']
        #    (c) anomaly of HF40
        hf_daily_clim = (
            hf_daily
            .groupby('doy')
            .agg(
                mean=(f'hf_{window_days}_anomaly', "mean"),
                std=(f'hf_{window_days}_anomaly', "std"),
                q20=(f'hf_{window_days}_anomaly', lambda x: x.quantile(0.2)),
                q80=(f'hf_{window_days}_anomaly', lambda x: x.quantile(0.8)),
            )
        )
        hf_daily = (
            hf_daily
            .reset_index()
            .merge(hf_daily_clim, how='inner', on='doy')
            .set_index('date')
            .dropna()
        )
        # 6) Convert to pandas DataFrame
        df_hf['doy'] = df_hf.index.day_of_year
        df_hf = (
            df_hf
            .reset_index()
            .merge(mean_hf, how='inner', on='doy')
            .merge(hf_daily_clim, how='inner', on='doy')
            .set_index('time')
            .dropna()
        )
        df_hf[f'hf_{window_days}_anomaly'] = df_hf[f'hf_{window_days}'] - df_hf['mean_hf']
        return df_hf