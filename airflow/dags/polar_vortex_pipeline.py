import xarray as xr
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.short_circuit import ShortCircuitOperator
from airflow.utils import timezone

## data
from app.tools.configs import Constant
from app.tools.plot_data import Plotter
from app.tools.loaders.era5 import ERA5_loader
from app.tools.loaders.gfs import GFS_loader
from app.tools.loaders.merger import Merger
from app.tools.loaders.exceptions import (
    TemporaryDataUnavailable,
    IncompleteDataError,
    FatalPipelineError,
    DataValidationError
)
## bot
import asyncio
from pathlib import Path
from app.tools.tg_bot.client import get_bot
from app.tools.tg_bot.sender import send_images
from app.tools.get_secrets import get_secret



## ---------------------------------------------------------------------
## DAG default args
## ---------------------------------------------------------------------
default_args = {
    "owner": "polar_vortex",
    "depends_on_past": False,
    "retries": 6,                          # 6 попыток
    "retry_delay": timedelta(minutes=10), # каждые 10 минут
    "execution_timeout": timedelta(minutes=7),  # одна попытка ≤ 7 минут
}


## ---------------------------------------------------------------------
## DAG definition
## ---------------------------------------------------------------------
with DAG(
    dag_id="polar_vortex_pipeline",
    description="Daily polar vortex data update and notification pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 3,9,15,20 * * *",   # 03, 09, 15, 20
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["polar_vortex"],
) as dag:

    ## -----------------------------------------------------------------
    ## Helpers / placeholders 
    ## -----------------------------------------------------------------
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


    ## -----------------------------------------------------------------
    ## ERA branch (09:00 only)
    ## -----------------------------------------------------------------
    def is_era_time():
        """
        ERA обновляем только в 09:00
        """
        now = timezone.utcnow()
        return now.hour == 9
    

    def cleanup_old_era():
            """
            Удаляем старые ERA файлы.
            Ошибки не фатальны.
            """
            loader = ERA5_loader()
            try:
                loader.remove_tmpfs()
            except Exception as e:
                pass


    def load_era():
        loader = ERA5_loader()

        try:
            loader.load()
            loader.merge()

        except TemporaryDataUnavailable:
            # данных ещё нет →  retry
            raise

        except IncompleteDataError:
            # частичная загрузка → чистим и retry
            loader.remove_tmpfs()
            raise

        except DataValidationError:
            # данные странные → retry 
            loader.remove_tmpfs()
            raise

        except Exception as e:
            loader.remove_tmpfs()
            raise FatalPipelineError("Fatal error in ERA pipeline") from e



    era_gate = ShortCircuitOperator(
        task_id="era_gate",
        python_callable=is_era_time,
    )

    cleanup_old_era_task = PythonOperator(
        task_id="cleanup_old_era",
        python_callable=cleanup_old_era,
    )

    load_era_task = PythonOperator(
        task_id="load_era",
        python_callable=load_era,
    )



    ## -----------------------------------------------------------------
    ## GFS tasks (always)
    ## -----------------------------------------------------------------
    def cleanup_old_gfs_fact_tmpfs():
        loader = GFS_loader()
        try:
            loader.remove_fact_tmpfs()
        except Exception as e:
            raise FatalPipelineError(
                "GFS remove fact tmpfs ERROR: pipeline is inconsistent"
            ) from e

    def cleanup_old_gfs_forecast_tmpfs():
        loader = GFS_loader()
        try:
            loader.remove_forecast_tmpfs()
        except Exception as e:
            raise FatalPipelineError(
                "GFS remove forcast tmpfs ERROR: pipeline is inconsistent"
            ) from e


    def load_gfs_fact():
        loader = GFS_loader()

        try:
            loader.load_fact()
            ds = loader.merge_fact_tmpfs()

            if ds is None:
                raise DataValidationError("GFS FACT merge returned None")

        except TemporaryDataUnavailable:
            # данных ещё нет →  retry
            raise

        except IncompleteDataError:
            # частичная загрузка → чистим и retry
            loader.remove_fact_tmpfs()
            raise

        except DataValidationError:
            # данные странные → retry 
            loader.remove_fact_tmpfs()
            raise

        except Exception as e:
            loader.remove_fact_tmpfs()
            raise FatalPipelineError("Fatal error in GFS FACT pipeline") from e


    def load_gfs_forecast():
        loader = GFS_loader()

        try:
            loader.load_forecast()
            ds = loader.merge_forecast_tmpfs()

            if ds is None:
                raise DataValidationError("GFS FORECAST merge returned None")

        except TemporaryDataUnavailable:
            # цикл ещё не вышел → retry
            raise

        except IncompleteDataError:
            # прогноз не докачался → чистим и retry
            loader.remove_forecast_tmpfs()
            raise

        except DataValidationError:
            # данные странные → retry 
            loader.remove_forecast_tmpfs()
            raise

        except Exception as e:
            loader.remove_forecast_tmpfs()
            raise FatalPipelineError("Fatal error in GFS FORECAST pipeline") from e


    load_gfs_fact_task = PythonOperator(
        task_id="load_gfs_fact",
        python_callable=load_gfs_fact,
    )

    load_gfs_forecast_task = PythonOperator(
        task_id="load_gfs_forecast",
        python_callable=load_gfs_forecast,
    )

    cleanup_old_gfs_fact_task = PythonOperator(
        task_id="cleanup_old_gfs_fact",
        python_callable=cleanup_old_gfs_fact_tmpfs,
    )
    
    cleanup_old_gfs_forecast_task = PythonOperator(
        task_id="cleanup_old_gfs_forecast",
        python_callable=cleanup_old_gfs_forecast_tmpfs,
    )

    ## -----------------------------------------------------------------
    ## Merge all datasets
    ## -----------------------------------------------------------------
    def merge_all():
        const = Constant()
        merger = Merger()

        try:
            era_data = xr.open_dataset(
                const.fact_dir / f"era_data_{const.today.date().strftime('%Y_%m_%d')}.nc"
            )
            gfs_fact = xr.open_dataset(
                const.fact_dir / "gfs_data.nc"
            )
            gfs_forecast = xr.open_dataset(
                const.forecast_dir /
                f"gfs_data_{const.today.date().strftime('%Y_%m_%d')}.nc"
            )

            ds = merger.merge_datasets(
                era_fact_xds=era_data,
                gfs_fact_xds=gfs_fact,
                gfs_forecast_xds=gfs_forecast,
            )
            #TODO переделать merger чтоб сохранял датасет итоговый
            out_path = (
                const.data_dir /
                f"merged_data_{const.today.date().strftime('%Y_%m_%d')}.nc"
            )
            out_path.parent.mkdir(parents=True, exist_ok=True)
            ds.to_netcdf(out_path)

        except Exception as e:
            raise FatalPipelineError(
                "Unexpected fatal error during merge_all"
            ) from e


    merge_all_task = PythonOperator(
        task_id="merge_all",
        python_callable=merge_all,
        trigger_rule="none_failed_min_one_success"
    )

    ## -----------------------------------------------------------------
    ## Build images
    ## -----------------------------------------------------------------

    def build_images():
        const = Constant()
        plotter = Plotter()

        try:
            ds = xr.open_dataset(
                const.data_dir /
                f"merged_data_{const.today.date().strftime('%Y_%m_%d')}.nc"
            )

            plotter.build_all(
                ds=ds,
                out_dir=const.images_dir,
            )

        except Exception as e:
            # всё остальное — баг в визуализации
            raise FatalPipelineError(
                "Unexpected fatal error during build_images"
            ) from e


    build_images_task = PythonOperator(
        task_id="build_images",
        python_callable=build_images,
    )


    ## -----------------------------------------------------------------
    ## Telegram notifications
    ## -----------------------------------------------------------------
    def send_images_to_telegram():
        const = Constant()

        try:
            bot = get_bot()
            chat_id = int(get_secret("TG_CHAT_ID"))
            images = sorted(const.images_dir.glob("*.png"))

            if not images:
                raise RuntimeError("No images found to send")

            asyncio.run(
                send_images(
                    bot=bot,
                    chat_id=chat_id,
                    images=images,
                    tag="#polar_vortex",
                )
            )

        except Exception as e:
            raise FatalPipelineError(
                "Failed to send images to Telegram"
            ) from e


    def send_failed_message_to_telegram():
        try:
            bot = get_bot()
            chat_id = int(get_secret("TG_CHAT_ID"))
            asyncio.run(
                bot.send_message(
                    chat_id=chat_id,
                    text="#failed_load\nData pipeline failed",
                )
            )

        except Exception:
            pass


    send_images_task = PythonOperator(
        task_id="send_images_to_telegram",
        python_callable=send_images_to_telegram,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    send_failed_task = PythonOperator(
        task_id="send_failed_message_to_telegram",
        python_callable=send_failed_message_to_telegram,
        trigger_rule=TriggerRule.ONE_FAILED,
    )


    # ## -----------------------------------------------------------------
    # ## Cleanup (always)
    # ## -----------------------------------------------------------------
    # def cleanup_gfs_tmp():
    #     pass


    # cleanup_gfs_tmp_task = PythonOperator(
    #     task_id="cleanup_gfs_tmp",
    #     python_callable=cleanup_gfs_tmp,
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )


    ## -----------------------------------------------------------------
    ## Dependencies
    ## -----------------------------------------------------------------

    start >> era_gate >> cleanup_old_era_task >> load_era_task
    start >> cleanup_old_gfs_fact_task >> load_gfs_fact_task
    start >> cleanup_old_gfs_forecast_task >> load_gfs_forecast_task

    [
        load_era_task,
        load_gfs_fact_task,
        load_gfs_forecast_task,
    ] >> merge_all_task

    merge_all_task >> build_images_task

    build_images_task >> send_images_task

    [
        load_era_task,
        load_gfs_fact_task,
        load_gfs_forecast_task,
        merge_all_task,
        build_images_task,
    ] >> send_failed_task

    send_images_task >> end
    send_failed_task >> end
