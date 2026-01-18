import pandas as pd
import cdsapi
import os
from pathlib import Path
from dataclasses import dataclass, field

import logging
from typing import Optional
from logging.handlers import RotatingFileHandler
from .get_secrets import get_secret

# ROOT_PATH = "/Users/artem/Documents/Zerich/polar_vortex/project_polar_vortex/app" 
ROOT_PATH = "/opt/airflow/app"



class LoadingFilter(logging.Filter):
    def filter(
            self, 
            record: logging.LogRecord
        ) -> bool:
        return getattr(record, "channel", None) == "loading"


@dataclass
class Logger:
    name: str
    log_dir: Path = Path(f"{ROOT_PATH}/data/logs")
    level: int = logging.DEBUG

    filename: str = "run.log"
    loading_filename: str = "loading.log"

    max_bytes: int = 20 * 1024 * 1024   # 20 MB
    backup_count: int = 5

    fmt: str = "%(asctime)s | %(levelname)-8s | %(message)s"

    _logger: Optional[logging.Logger] = field(init=False, default=None)


    ## -------------------------------------------------------------------------------- 
    ## lazy init 
    ## -------------------------------------------------------------------------------- 
    def _ensure_logger(self):
        if self._logger is not None:
            return

        self.log_dir.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        logger.propagate = False

        if not logger.handlers:
            formatter = logging.Formatter(self.fmt)
            ## --------------------------------------- 
            ## main rotating handler 
            ## --------------------------------------- 
            main_handler = RotatingFileHandler(
                self.log_dir / self.filename,
                maxBytes=self.max_bytes,
                backupCount=self.backup_count,
            )
            main_handler.setFormatter(formatter)
            main_handler.setLevel(self.level)
            ## --------------------------------------- 
            ## loading handler 
            ## --------------------------------------- 
            loading_handler = logging.FileHandler(
                self.log_dir / self.loading_filename
            )
            loading_handler.setFormatter(formatter)
            loading_handler.setLevel(self.level)
            loading_handler.addFilter(LoadingFilter())
            ## --------------------------------------- 
            ## console 
            ## --------------------------------------- 
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            stream_handler.setLevel(self.level)

            logger.addHandler(main_handler)
            logger.addHandler(loading_handler)
            logger.addHandler(stream_handler)

        self._logger = logger


    ## -------------------------------------------------------------------------------- 
    ## public API 
    ## -------------------------------------------------------------------------------- 
    def info(self, msg: str):
        self._ensure_logger()
        self._logger.info(msg)

    def debug(self, msg: str):
        self._ensure_logger()
        self._logger.debug(msg)

    def loading(self, msg: str):
        self._ensure_logger()
        self._logger.info(
            msg,
            extra={"channel": "loading"}
        )

    def loading_error(self, msg: str):
        self._ensure_logger()
        self._logger.error(
            msg,
            extra={"channel": "loading"},
        )




@dataclass
class Constant:
    ## ----------------------------------------------------------------------
    ## Dates
    ## ----------------------------------------------------------------------
    today: pd.Timestamp = pd.Timestamp.today()
    start_date: pd.Timestamp = today - pd.Timedelta(days=90)
    ## ----------------------------------------------------------------------
    ## Dirs
    ## ----------------------------------------------------------------------
    root_dir: Path = Path(ROOT_PATH)
    data_dir: Path = root_dir / "data" / 'datasets'
    fact_dir: Path = root_dir / "data" / 'datasets' / "fact"
    forecast_dir: Path = root_dir / "data" / 'datasets' / "forecast"
    images_dir: Path = root_dir / "data" / "images"
    ## ----------------------------------------------------------------------
    ## Climate data
    ## ----------------------------------------------------------------------
    hf_daily_csv_path: str = root_dir / 'data' / 'climatology' / 'hf_daily.csv'



@dataclass
class ERA_const:
    ## ----------------------------------------------------------------------
    ## CDS client
    ## ----------------------------------------------------------------------
    url: str = "https://cds.climate.copernicus.eu/api"
    # key: str = field(default_factory=lambda: ERA_const.get_env("CDS_API_KEY"))
    key: str = field(default_factory=lambda: get_secret("CDS_API_KEY"))
    verify: int = 1 
    quiet: bool = False
    timeout: int = 10
    retry_max: int = 50
    sleep_max: int = 5 
    ## ----------------------------------------------------------------------
    ## Datasets configs
    ## ----------------------------------------------------------------------
    cleanup_tmpfs: bool = True
    era_reanalisys_delay: int = 5 ## 5 days delay
    dataset: str = "reanalysis-era5-pressure-levels"
    request: dict = field(
        default_factory = lambda: (
            {
                "product_type": "reanalysis",
                "pressure_level": [
                    "10", "100",
                ],
                "variable": [
                    "u_component_of_wind",
                    "v_component_of_wind",
                    "temperature",
                ],
                # "year": str(year),
                # "month": months,
                "day": [f"{d:02d}" for d in range(1, 32)],
                "time": ["00:00"],  
                "area": [90, -180, 45, 180], 
                # "area": [75, -180, 45, 180], 
                "data_format": "netcdf",
                "download_format": "unarchived",
            }  
        )
    )

    @staticmethod
    def get_env(name: str) -> str:
        value = os.getenv(name)
        if value is None:
            raise RuntimeError(f"Environment variable {name} is not set")
        return value


    


@dataclass
class GFS_const:
    base_url: str = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    ## ----------------------------------------------------------------------
    ## Datasets configs
    ## ----------------------------------------------------------------------
    pressure_levels: tuple[int, ...] = (10, 50, 100)
    request: dict = field(
        default_factory = lambda: (
            {
                ## vars
                "var_UGRD": "on",
                "var_VGRD": "on",
                "var_TMP": "on",
                ## region
                "leftlon": str(0),
                "rightlon": str(360),
                "toplat": str(90),
                "bottomlat": str(45),
                ## pressure levels
                **{f"lev_{pl}_mb": "on" for pl in (10, 50, 100)},
            }  
        )
    )
    cleanup_tmpfs: bool = True
    era_reanalisys_delay: int = 5 ## ERA5 configs class
    ## --- FACT --- ##
    fact_cycle: str = '00'
    ## --- FORECAST --- ##
    forecast_cycle: str = "auto" #"auto" #"06"
    forecast_days: int = 14
    forecast_step_hours: int = 6
