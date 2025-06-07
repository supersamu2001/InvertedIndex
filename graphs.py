import matplotlib.pyplot as plt

'''
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
'''
# Inserisci le dimensioni dei dataset (valori comuni per entrambe le funzioni)
x_values = [0.25, 94, 353, 706, 1100, 2200]

# Tempi di esecuzione per la Funzione 1
y_hadoop_combiner = [21970, 101067, 275230, 546362, 1513755, 2569199]

y_hadoop_inmapper = [16067, 64016, 234068, 306486, 355855, 1923344]

y_spark = [ 55901, 140189, 335665, 615965, 1058833, 1702367]

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