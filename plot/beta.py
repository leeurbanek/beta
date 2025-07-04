import logging, logging.config

DEBUG = True
logging.config.fileConfig(fname='../logger.ini')
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# sns.set_theme(style="ticks")

# # Create a dataset with many short random walks
# rs = np.random.RandomState(4)
# pos = rs.randint(-1, 2, (20, 5)).cumsum(axis=1)
# pos -= pos[:, 0, np.newaxis]
# step = np.tile(range(5), 20)
# walk = np.repeat(range(20), 5)
# df = pd.DataFrame(np.c_[pos.flat, step, walk],
#                   columns=["position", "step", "walk"])

# print(df)

# # Initialize a grid of plots with an Axes for each walk
# grid = sns.FacetGrid(
#     df, col="walk", hue="walk", palette="tab20c", col_wrap=4, height=1.5
# )

# # Draw a line plot to show the trajectory of each random walk
# grid.map(plt.plot, "step", "position")

# # Adjust the tick positions and labels
# grid.set(
#     xticks=np.arange(5), yticks=[-3, 3], xlim=(-.5, 4.5), ylim=(-3.5, 3.5)
# )

# # Adjust the arrangement of the plots
# grid.fig.tight_layout(w_pad=1)

# plt.show()

# =======

flights = sns.load_dataset("flights")
if DEBUG: logger.debug(f"flights:\n{flights} {type(flights)}")

# Plot each year's time series in its own facet
g = sns.relplot(
    data=flights,
    x="month", y="passengers", col="year", hue="year",
    kind="line", palette="crest", linewidth=4, zorder=5,
    col_wrap=3, height=2, aspect=1.5, legend=False,
)
if DEBUG: logger.debug(f"grid: {g}\naxes_dict: {g.axes_dict}")

# # Iterate over each subplot to customize further
# for year, ax in g.axes_dict.items():

#     # Add the title as an annotation within the plot
#     ax.text(.8, .85, year, transform=ax.transAxes, fontweight="bold")

#     # Plot every year's time series in the background
#     sns.lineplot(
#         data=flights, x="month", y="passengers", units="year",
#         estimator=None, color=".7", linewidth=1, ax=ax,
#     )

# # Reduce the frequency of the x axis ticks
# ax.set_xticks(ax.get_xticks()[::2])

# # Tweak the supporting aspects of the plot
# g.set_titles("")
# g.set_axis_labels("", "Passengers")
# g.tight_layout()

# =======

# g = sns.relplot(data = df_monthly, x = "YM", y = "PM2.5",
#                 col = "District", hue = "District",
#                 kind = "line", palette = "Spectral",
#                 linewidth = 4, zorder = 5,
#                 col_wrap = 5, height = 3, aspect = 1.5, legend = False
#                )

# #add text and silhouettes
# for time, ax in g.axes_dict.items():
#     ax.text(.1, .85, time,
#             transform = ax.transAxes, fontweight="bold"
#            )
#     sns.lineplot(data = df_monthly, x = "YM", y = "PM2.5", units="District",
#                  estimator = None, color= ".7", linewidth=1, ax=ax
#                 )

# ax.set_xticks('')
# g.set_titles("")
# g.set_axis_labels("", "PM2.5")
# g.tight_layout()
