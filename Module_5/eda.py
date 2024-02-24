import numpy as np
import scipy.stats as stats 
from tabulate import tabulate
import matplotlib.pyplot as plt

from IPython.display import display, HTML, Markdown

def display_markdown(df):
    table = tabulate(df, showindex=False, headers=df.columns, floatfmt='.2f', tablefmt='simple')
    return Markdown(table)

def display_dict(m, precision = 3):
    table = "<table>"
    for item in m.items():
        table += ("<tr><th>{0}</th><td>{1:." + str(precision) + "f}</td></tr>").format(*item)
    table += "</table>"
    return display(HTML(table))

def calculate_tukey_five(data):
    values = list(np.concatenate([[np.min(data)], stats.mstats.mquantiles( data, [0.25, 0.5, 0.75]),[np.max(data)]]))
    labels = ["Min", "Q1", "Median", "Q3", "Max"]
    data = {"Stats": labels, "Values": values}
    return data

def calculate_tukey_dispersion(five):
    _five = {k: v for k, v in zip(five["Stats"], five["Values"])}
    labels = ["Range", "IQR", "QCV"]
    values = [
        _five["Max"] - _five["Min"],
        _five["Q3"] - _five["Q1"],
        (_five["Q3"] - _five["Q1"]) / _five["Median"]
    ]
    return {"Stats": labels, "Values": values}

def tukey(data):
    five = calculate_tukey_five(data)
    dispersion = calculate_tukey_dispersion(five)
    return {"Stats": five["Stats"] + dispersion["Stats"], "Values": five["Values"] + dispersion["Values"]}

def restyle_boxplot(patch):
    ## change color and linewidth of the whiskers
    for whisker in patch['whiskers']:
        whisker.set(color='#000000', linewidth=1)

    ## change color and linewidth of the caps
    for cap in patch['caps']:
        cap.set(color='#000000', linewidth=1)

    ## change color and linewidth of the medians
    for median in patch['medians']:
        median.set(color='#000000', linewidth=2)

    ## change the style of fliers and their fill
    for flier in patch['fliers']:
        flier.set(marker='o', color='#000000', alpha=0.2)

    for box in patch["boxes"]:
        box.set( facecolor='#FFFFFF', alpha=0.5)


def freeman_diaconis(data):
    mn = data.min()
    mx = data.max()
    quartiles = stats.mstats.mquantiles( data, [0.25, 0.5, 0.75])
    iqr = quartiles[2] - quartiles[ 0]
    n = len( data)
    h = 2.0 * (iqr/n**(1.0/3.0))
    return int(np.ceil((mx - mn)/h)), mn, mx

def histogram_w_whiskers(data, variable_name, bins=None, zoom=None):
    k, mn, mx = freeman_diaconis(data[variable_name])
    if not bins:
        bins = np.linspace(mn, mx, num=k)
        print(f"Freeman Diaconis for {variable_name}: {len(bins)} bins")

    observations = len(data)
    empirical_weights = np.ones(observations)/observations # this converts counts to relative frequencies when used in hist()
        
    # start the plot: 2 rows, because we want the boxplot on the first row
    # and the hist on the second
    fig, ax = plt.subplots(
        2, figsize=(7, 5), sharex=True,
        gridspec_kw={"height_ratios": (.7, .3)}  # the boxplot gets 30% of the vertical space
    )

    # the histogram
    ax[0].hist(data[variable_name],bins=bins, color="dimgray", weights=empirical_weights)
    ax[0].set_title(f"{variable_name} distribution - Freeman Diaconis")
    ax[0].set_ylabel("Relative Frequency")
    if zoom:
        ax[0].set_ylim((0, zoom))
    # the box plot
    ax[1].boxplot(data[variable_name], vert=False)
    # removing borders
    ax[1].spines['top'].set_visible(False)
    ax[1].spines['right'].set_visible(False)
    ax[1].spines['left'].set_visible(False)
    ax[1].set_xlabel(variable_name)

    # and we are good to go
    plt.show()
    plt.close()
#

def histogram_trio(data, variable_name, bins=None, fewer_bins=None, more_bins=None, zoom=1.0):
    k, mn, mx = freeman_diaconis(data[variable_name])
    bins = np.linspace(mn, mx, num=k) if not bins else bins #[i for i in range( mn, mx, h)]
    print(f"Freeman Diaconis for {variable_name}: {len(bins)} bins")

    observations = len(data)
    empirical_weights = np.ones(observations)/observations # this converts counts to relative frequencies when used in hist()

    fig, ax = plt.subplots(1, 3, figsize=(20, 6), sharey=True)

    fewer_bins = int(len(bins) * .50) if not fewer_bins else fewer_bins
    more_bins = int(len(bins) * 2) if not more_bins else more_bins

    n, bins, patches = ax[1].hist(data[variable_name], color="DimGray", bins=bins, weights=empirical_weights) # <---
    ax[1].set_xlabel(variable_name)
    ax[1].set_ylabel("Relative Frequency")
    ax[1].set_title(f"Relative Frequency Histogram of {variable_name}")
    ax[1].set_ylim((0, zoom))
    
    n, bins, patches = ax[0].hist(data[variable_name], color="DimGray", bins=fewer_bins, weights=empirical_weights)
    ax[0].set_xlabel(variable_name)
    ax[0].set_ylabel("Relative Frequency")
    ax[0].set_title(f"Relative Frequency Histogram of {variable_name} (Fewer Bins)")
    
    n, bins, patches = ax[2].hist(data[variable_name], color="DimGray", bins=more_bins, weights=empirical_weights)
    ax[2].set_xlabel(variable_name)
    ax[2].set_ylabel("Relative Frequency")
    ax[2].set_title(f"Relative Frequency Histogram of {variable_name} (More Bins)")

    plt.show()
    plt.close()
#