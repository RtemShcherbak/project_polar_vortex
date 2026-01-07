import matplotlib.pyplot as plt
import pandas as pd
from configs import Constant, Logger
from typing import List, Dict, Union
from pathlib import Path


class Plotter:
    def __init__(self):
        self.logger = Logger(name='data_loading_logger')
        self.const = Constant()
        
    ## ------------------------------------------------------------------------
    ## Private
    ## ------------------------------------------------------------------------

    ## ------------------------------------------------------------------------
    ## Public
    ## ------------------------------------------------------------------------
    def plot_graph(
        self,
        df: pd.DataFrame,
        var_name: str,
        ylabel: str,
        xlabel: str,
        title: str,
        fill_between: bool = False,
        show_ssw_25: bool = False,
        show_today: bool = True,
        save_dir: Path | None = None,
        filename: str | None = None,
        show: bool = True,
    ):
        fig, ax = plt.subplots(1, 1, figsize=[20, 6])

        # -----------------------------
        # базовые проверки
        # -----------------------------
        if var_name not in df.columns:
            raise KeyError(f"Column '{var_name}' not found in DataFrame")

        has_mean = "mean" in df.columns
        has_q = {"q20", "q80"}.issubset(df.columns)

        self.logger.debug(
            f"Plotting '{var_name}' | mean={has_mean}, q20/q80={has_q}"
        )

        # -----------------------------
        # статистика (если есть)
        # -----------------------------
        if fill_between:
            if has_mean:
                ax.plot(
                    df.index,
                    df["mean"],
                    "k--",
                    lw=2,
                    label="climatology mean",
                )
        else:
            self.logger.debug("fill_between=True but 'mean' column — skipping mean line")

        if fill_between:
            if has_q:
                ax.fill_between(
                    df.index,
                    df["q20"].to_numpy(),
                    df["q80"].to_numpy(),
                    color="0.85",
                    alpha=1.0,
                    label="q20–q80",
                )
            else:
                self.logger.debug(
                    "fill_between=True but q20/q80 not found — skipping"
                )

        # -----------------------------
        # основной временной ряд
        # -----------------------------
        ymin = df[var_name].min()
        ymax = df[var_name].max()

        today = self.const.today - pd.Timedelta(hours=12)

        ax.plot(
            df[df.index < today].index,
            df[df.index < today][var_name],
            lw=2,
            label=f"{var_name} history",
        )

        ax.plot(
            df[df.index >= (today - pd.Timedelta(days=1))].index,
            df[df.index >= (today - pd.Timedelta(days=1))][var_name],
            lw=2,
            label=f"{var_name} forecast",
        )

        # -----------------------------
        # маркеры
        # -----------------------------
        if show_today:
            ax.vlines(
                today-pd.Timedelta(days=0, hours=12), 
                ymin=ymin, ymax=ymax, 
                color="grey", alpha=0.35, lw=1.2, 
                label="Current date"
            )
            ax.scatter(
                today-pd.Timedelta(days=0, hours=12), 
                ymax, 
                color="grey", s=22, alpha=0.8, zorder=4
            )
            ax.scatter(
                today-pd.Timedelta(days=0, hours=12), 
                ymin, 
                color="grey", s=22, alpha=0.8, zorder=4
            )


        if show_ssw_25:
            d = pd.Timestamp("2025-11-28")
            ax.vlines(
                d, 
                ymin=ymin, ymax=ymax, 
                color="red", alpha=0.35, lw=1.2, 
                label="SSW 28-11-2025"
            )
            ax.scatter(
                d, 
                ymax, 
                color="red", s=22, alpha=0.8, zorder=4
            )
            ax.scatter(
                d, 
                ymin, 
                color="red", s=22, alpha=0.8, zorder=4
            )

        # -----------------------------
        # оформление
        # -----------------------------
        ax.set_ylabel(ylabel, fontsize=14)
        ax.set_xlabel(xlabel, fontsize=14)
        ax.set_title(title, fontsize=18)

        ax.grid()
        ax.legend()


        # -----------------------------
        # сохранение
        # -----------------------------
        if save_dir is not None:
            save_dir = Path(save_dir)
            save_dir.mkdir(parents=True, exist_ok=True)

            if filename is None:
                filename = f"{var_name}.png"

            out_path = save_dir / filename
            fig.savefig(out_path, dpi=150, bbox_inches="tight")

            self.logger.info(f"Saved plot to {out_path}")

        # -----------------------------
        # show / close
        # -----------------------------
        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig, ax

