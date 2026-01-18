import xarray as xr
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
# from airflow.operators.short_circuit import ShortCircuitOperator
from airflow.utils import timezone
import pendulum

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
from app.tools.data_prepoc import Vars
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
    "retries": 10,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),  # одна попытка
}


## ---------------------------------------------------------------------
## DAG definition
## ---------------------------------------------------------------------
local_tz = pendulum.timezone("Europe/Moscow")

with DAG(
    dag_id="polar_vortex_pipeline",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule="0 2,8,14,20 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
):

    ## -----------------------------------------------------------------
    ## Helpers / placeholders 
    ## -----------------------------------------------------------------
    start = EmptyOperator(
        task_id="start"
    )
    end = EmptyOperator(
        task_id="end",
        trigger_rule="all_done",
    )


    ## -----------------------------------------------------------------
    ## ERA branch (09:00 only)
    ## -----------------------------------------------------------------
    def is_era_time():
        now = datetime.now(timezone.utc)
        return True #now.hour == 9


    # def cleanup_old_era():
    #         """
    #         Удаляем старые ERA файлы.
    #         Ошибки не фатальны.
    #         """
    #         if not is_era_time:
    #             return "ERA already available"
    #         loader = ERA5_loader()
    #         try:
    #             loader.remove_tmpfs()
    #         except Exception as e:
    #             pass


    def load_era():
        if not is_era_time():
            return "ERA already available"

        loader = ERA5_loader()

        try:
            loader.remove_tmpfs()
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
            raise AirflowFailException("Fatal error in ERA pipeline") from e




    # era_gate = ShortCircuitOperator(
    #     task_id="era_gate",
    #     python_callable=is_era_time,
    # )

    # cleanup_old_era_task = PythonOperator(
    #     task_id="cleanup_old_era",
    #     python_callable=cleanup_old_era,
    # )

    load_era_task = PythonOperator(
        task_id="load_era",
        python_callable=load_era,
    )

    # era_ready = EmptyOperator(
    #     task_id="era_ready",
    #     trigger_rule="none_failed_min_one_success"
    # )


    ## -----------------------------------------------------------------
    ## GFS tasks (always)
    ## -----------------------------------------------------------------
    # def cleanup_old_gfs_fact_tmpfs():
    #     loader = GFS_loader()
    #     try:
    #         loader.remove_fact_tmpfs()
    #     except Exception as e:
    #         raise FatalPipelineError(
    #             "GFS remove fact tmpfs ERROR: pipeline is inconsistent"
    #         ) from e

    # def cleanup_old_gfs_forecast_tmpfs():
    #     loader = GFS_loader()
    #     try:
    #         loader.remove_forecast_tmpfs()
    #     except Exception as e:
    #         raise FatalPipelineError(
    #             "GFS remove forcast tmpfs ERROR: pipeline is inconsistent"
    #         ) from e


    def load_gfs_fact():
        loader = GFS_loader()

        try:
            loader.remove_fact_tmpfs()
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
            raise AirflowFailException("Fatal error in GFS FACT pipeline") from e


    def load_gfs_forecast():
        loader = GFS_loader()

        try:
            loader.remove_forecast_tmpfs()
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
            raise AirflowFailException("Fatal error in GFS FORECAST pipeline") from e


    load_gfs_fact_task = PythonOperator(
        task_id="load_gfs_fact",
        python_callable=load_gfs_fact,
    )

    load_gfs_forecast_task = PythonOperator(
        task_id="load_gfs_forecast",
        python_callable=load_gfs_forecast,
    )

    # cleanup_old_gfs_fact_task = PythonOperator(
    #     task_id="cleanup_old_gfs_fact",
    #     python_callable=cleanup_old_gfs_fact_tmpfs,
    # )
    
    # cleanup_old_gfs_forecast_task = PythonOperator(
    #     task_id="cleanup_old_gfs_forecast",
    #     python_callable=cleanup_old_gfs_forecast_tmpfs,
    # )

    ## -----------------------------------------------------------------
    ## Merge all datasets
    ## -----------------------------------------------------------------
    def merge_all():
        const = Constant()
        merger = Merger()

        try:
            era_data = xr.open_dataset(
                const.fact_dir / f"era_data.nc"
            )
            gfs_fact = xr.open_dataset(
                const.fact_dir / "gfs_data.nc"
            )
            gfs_forecast = xr.open_dataset(
                const.forecast_dir / f"gfs_data.nc"
            )

            ds = merger.merge_datasets(
                era_fact_xds=era_data,
                gfs_fact_xds=gfs_fact,
                gfs_forecast_xds=gfs_forecast,
            )
            #TODO переделать merger чтоб сохранял датасет итоговый
            out_path = (const.data_dir / f"merged_data.nc")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            ds.to_netcdf(out_path)

        except Exception as e:
            raise AirflowFailException(
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
        preproc = Vars()
        const = Constant()
        plotter = Plotter()

        try:
            ds = xr.open_dataset(
                const.data_dir /
                f"merged_data.nc"
            )

            u_df = preproc.get_u60_df(ds)
            t_df = preproc.get_t_df(ds)
            eddy_df = preproc.get_eddy_hf_df(ds)

            plotter.plot_graph(
                u_df,
                var_name='u',
                ylabel="Wind speed (m/s)",
                xlabel="Date",
                title="Zonal mean wind U(60°N, 10 hPa)",
                show=True,
                save_image=True,
            )
            plotter.plot_graph(
                t_df,
                var_name='t_C',
                ylabel="Temperature (°C)",
                xlabel="Date",
                title="Polar cap temperature (60–90°N, 10 hPa)",
                show=True,
                save_image=True,
            )
            plotter.plot_graph(
                eddy_df,
                var_name='hf_40_anomaly',
                ylabel="Heat flux anomaly (K·m/s)",
                xlabel="Date",
                title="Eddy heat flux anomaly (40-day running mean)",
                show=True,
                show_today=True,
                save_image=True,
            )

        except Exception as e:
            # всё остальное — баг в визуализации
            raise AirflowFailException(
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
            images = sorted(
                const.images_dir.glob("*.png"),
                key=lambda p: p.stat().st_mtime
            )[-3:]

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
            raise AirflowFailException(
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
        # trigger_rule=TriggerRule.ONE_FAILED,
        trigger_rule="one_failed"
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

    start >> load_era_task
    start >> load_gfs_fact_task
    start >> load_gfs_forecast_task

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
