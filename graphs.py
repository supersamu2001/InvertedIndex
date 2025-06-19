import matplotlib.pyplot as plt
import numpy as np


def time_comparison():
    # Inserisci le dimensioni dei dataset (valori comuni per entrambe le funzioni)
    x_values = [0.25, 94, 353, 706, 1100, 2200]

    # KB, MB1, MB2, MB3, GB1, GB2
    # Tempi di esecuzione per la Funzione 1
    y_hadoop_combiner = [13.5, 111, 181, 289, 571, 790]

    y_hadoop_inmapper = [13, 87, 168.5, 187, 464, 663]

    # Tempi di esecuzione per la Funzione 2
    y_nonParallel = [0.1, 0.5, 133, 202, 830, 1968]

    y_spark = [7, 34, 108, 210, 378, 606]

    plt.figure(figsize=(10, 6))

    # Tracciamo le due funzioni
    plt.plot(x_values, y_hadoop_combiner, marker='o', label='Hadoop with combiner function', color='blue')
    plt.plot(x_values, y_hadoop_inmapper, marker='*', label='Hadoop with in-mapper function', color='yellow')
    plt.plot(x_values, y_nonParallel, marker='s', label='Non-parallel function', color='green')
    plt.plot(x_values, y_spark, marker='^', label='Spark function', color='red')

    # Aggiungiamo etichette e titolo
    plt.xlabel("Dataset size (MB)")
    plt.ylabel("Elapsed time (s)")
    plt.title("Time comparison")
    plt.legend()
    plt.grid(True)

    # Mostra il grafico
    plt.tight_layout()
    plt.savefig("Hadoop-Spark-NonParallel (time).png", dpi=300)  # dpi=300 per alta qualità
    plt.show()

def resources_comparison():
    # Inserisci le dimensioni dei dataset (valori comuni per entrambe le funzioni)
    x_values = [0.25, 94, 353, 706, 1100, 2200]

    # Tempi di esecuzione per la Funzione 1
    y_hadoop_combiner = [21970, 101067, 275230, 546362, 1513755, 2569199]

    y_hadoop_inmapper = [16067, 64016, 234068, 306486, 355855, 1923344]

    y_spark = [55901, 140189, 335665, 615965, 1058833, 1702367]

    plt.plot(x_values, y_hadoop_combiner, marker='o', label='Hadoop with combiner function', color='blue')
    plt.plot(x_values, y_hadoop_inmapper, marker='*', label='Hadoop with in-mapper function', color='yellow')
    plt.plot(x_values, y_spark, marker='^', label='Spark function', color='red')

    plt.xlabel("Dataset size (MB)")
    plt.ylabel("Aggregate Resource Allocation (MB-seconds)")
    plt.title("resource comparison")
    plt.legend()
    plt.grid(True)

    # Mostra il grafico
    plt.tight_layout()
    plt.savefig("Hadoop-Spark (resources).png", dpi=300)  # dpi=300 per alta qualità
    plt.show()

def external_vs_inmapper():
    x_values = [0.25, 94, 353, 706, 1100, 2200]
    y_hadoop_combiner = [13.67, 111.67, 181.50, 289, 571, 790]
    y_hadoop_inmapper = [13, 87, 168.50, 187.5, 464, 663]

    plt.plot(x_values, y_hadoop_combiner, marker='o', label='Hadoop with combiner function', color='blue')
    plt.plot(x_values, y_hadoop_inmapper, marker='*', label='Hadoop with in-mapper function', color='yellow')

    plt.xlabel("Dataset size (MB)")
    plt.ylabel("Elapsed time (s)")
    plt.title("time comparison")
    plt.legend()
    plt.grid(True)

    # Mostra il grafico
    plt.tight_layout()
    plt.savefig("externalVsInmapper.png", dpi=300)  # dpi=300 per alta qualità
    plt.show()

def more_reducers():
    x_values = [1, 2, 6, 8, 12, 16, 20]
    y_1gb_1 = [464, 4*60+37, 5*60+28, 4*60+45, 5*60+38, 5*60+19, 5*60+43]
    y_1gb_2 = [478, 5*60+37, 4*60+46, 4*60+34, 4*60+24, 5*60+14, 5*60+12]
    y_1gb_3 = [450, 4*60+50, 4*60+47, 4*60+41, 4*60+43, 5*60+18, 4*60+45]

    y_2gb_1 = [663, 9*60+25, 6*60+49, 6*60+33, 7*60+59, 7*60+48, 7*60+43]
    y_2gb_2 = [11*60+21, 9*60, 6*60+52, 6*60+10, 6*60+46, 8*60+5, 8*60+22]
    y_2gb_3 = [672, 8*60+38, 7*60+4, 6*60+53, 6*60+50, 7*60+32, 7*60+28]

    # Combine in a 2D array for convenient computing
    y_vals = np.array([y_1gb_1, y_1gb_2, y_1gb_3])

    # Calculate mean and standard deviation along the first axis
    mean_vals = np.mean(y_vals, axis=0)
    std_vals = np.std(y_vals, axis=0)

    y_vals_2gb = np.array([y_2gb_1, y_2gb_2, y_2gb_3])

    mean_vals_2gb = np.mean(y_vals_2gb, axis=0)
    std_vals_2gb = np.std(y_vals_2gb, axis=0)

    # Plot with error bars
    plt.errorbar(x_values, mean_vals, yerr=std_vals, fmt='o-', capsize=5, label='1 GB')
    plt.errorbar(x_values, mean_vals_2gb, yerr=std_vals_2gb, fmt='s-', capsize=5, label='2 GB')

    plt.xticks(x_values)

    plt.xlabel('Number of reducers')
    plt.ylabel('Elapsed time (s)')
    plt.ylim(0)
    plt.title('Average and standard deviation on 3 runs')

    plt.grid()
    plt.legend()
    plt.savefig("moreReducers.png", dpi=300)  # dpi=300 per alta qualità
    plt.show()

def different_split():
    import matplotlib.pyplot as plt
    import numpy as np

    # Dataset size (MB)
    x_values = [94, 353, 706, 1100]

    # Tempi di esecuzione per ogni configurazione
    y_hadoop_combiner_128 = [111.67, 181.50, 289, 571]
    y_hadoop_combiner_1 = [216, 545, 1021, 1697]

    # Larghezza delle barre e posizioni
    bar_width = 0.35
    x_indexes = np.arange(len(x_values))

    # Creazione del grafico a colonne
    plt.bar(x_indexes - bar_width/2, y_hadoop_combiner_128, width=bar_width,
            label='Hadoop with 128MB per split', color='blue')
    plt.bar(x_indexes + bar_width/2, y_hadoop_combiner_1, width=bar_width,
            label='Hadoop with 1MB per split', color='red')

    # Etichette e titoli
    plt.xlabel("Dataset size (MB)")
    plt.ylabel("Time (in seconds)")
    plt.title("Different input splits (Bar Chart)")
    plt.xticks(ticks=x_indexes, labels=[str(v) for v in x_values])
    plt.legend()
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)

    # Layout e salvataggio
    plt.tight_layout()
    plt.savefig("Different_inputSplit.png", dpi=300)
    plt.show()

# external_vs_inmapper()
different_split()
