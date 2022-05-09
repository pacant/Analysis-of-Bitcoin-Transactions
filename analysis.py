from heapq import merge
import pandas as pd
from matplotlib import pyplot
# da csv a dataframe
inputs = pd.read_csv("inputs.csv", index_col=[0])
outputs = pd.read_csv("outputs.csv", index_col=[0])
transactions = pd.read_csv("transactions.csv", index_col=[0])

# join tabella input con output precedenti, drop colonne duplicate
io = inputs.merge(outputs, left_on="prev_out_id",
                  right_on="out_id", how="left").drop(["prev_out_id", "pk_id"], axis=1)


# riempio i campi NaN con -2
io.fillna(-2, inplace=True)
io = io.astype(int)

# join di input e output precedenti con output successivi, drop colonne duplicate
oio = io.merge(outputs, left_on="tx_id_x", right_on="tx_id",
               how="outer").drop("tx_id", axis=1)

# transazioni da rimuovere
tx_remove = []
# input che si riferiscono ad ouput precedenti non esistenti
invalid_input = oio.query("sig_id != 0 & out_id_x == -2")
tx_remove.extend(invalid_input["tx_id_x"].values)
print("INVALID INPUT:")
print(invalid_input)

# -2 sui campi vuoti, necessario per trasformare in int
oio = oio.fillna(-2)
oio = oio.astype(int)
# oio : input con output precedenti e nuovi corrispondenti

ds = io.query("tx_id_x != -2")
ds = ds.query("out_id != -2")

# cerco double spendings.
ds = ds.groupby("out_id").filter(lambda g: len(g) >
                                 1).drop_duplicates(subset="out_id", keep="last")

tx_remove.extend(ds["tx_id_x"].values)
print("DOUBLE SPENDINGS:")
print(ds)

# transazioni con valore in output negativo
neg_tx = oio.query(
    "value_y < 0 & value_y != -2")

tx_remove.extend(neg_tx["tx_id_x"].values)
print("NEGATIVE TX: ")
print(neg_tx)

# transazioni con input > output
i_amount = (io.query("value != -2")).groupby("tx_id_x")["value"].sum()
o_amount = outputs.groupby("tx_id")["value"].sum()

merged_fees = pd.merge(i_amount, o_amount, left_index=True, right_index=True)

merged_fees["fees"] = merged_fees["value_x"] - merged_fees["value_y"]
neg_fees = merged_fees[merged_fees["fees"] < 0]
tx_remove.extend(neg_fees.index.values)

# rimuovo a catena le transazioni correlate a quelle invalide
##
tx_queue = tx_remove.copy()
results = []

while tx_queue != []:
    tx_id = tx_queue.pop(0)
    # output transazioni invalide
    outputs_tx = outputs[outputs["tx_id"] == tx_id]["out_id"].values
    # transazioni da rimuovere a catena che prendono come input quelle invalide
    chain = inputs[inputs["prev_out_id"].isin(outputs_tx)]["tx_id"].values

    for tx in chain:
        if tx in results:
            # rimuovo transazioni già esaminate
            chain = list(filter(lambda x: x != tx, chain))
    tx_queue = tx_queue + list(set(chain))
    results = results + list(set(chain))

results = results + list(set(tx_remove))

# rimuovo la catena di transazioni dal dataset
transactions = transactions[~transactions["tx_id"].isin(results)]
inputs = inputs[~inputs["tx_id"].isin(results)]
outputs = outputs[~outputs["tx_id"].isin(results)]
oio = oio[~oio["tx_id_x"].isin(results)]
merged_fees = merged_fees[~merged_fees.index.isin(results)]

# uxto transactions, max_uxto: transazione con max uxto
uxto = outputs.merge(inputs, right_on="prev_out_id",
                     left_on="out_id", how="left")

uxto = uxto.fillna(-3)
uxto = uxto.astype(int)
# cerco solo le colonne con input nullo, ovvero output che non vengono spesi. (perchè ho fatto il left join)
uxto = uxto[uxto["in_id"] == -3]
uxto = uxto.drop(["sig_id", "in_id", "tx_id_y", "prev_out_id"], axis=1)
uxto = uxto.merge(transactions, left_on="tx_id_x", right_on="tx_id")
uxto.drop(["tx_id_x"], axis=1, inplace=True)
max_uxto = uxto[uxto["value"] == uxto["value"].max()]

print("MAX UXTO: ")
print(max_uxto)

# Numero di transazioni per ogni blocco
count_blocks = transactions.groupby(
    "block_id")["tx_id"].count().reset_index(name="num_tx")
# raggruppo le transazioni per ogni mese. Il periodo di tempo è circa 24 mesi, circa 4168 blocchi al mese
count_blocks = count_blocks.groupby(
    count_blocks.index // 4168)["num_tx"].sum().reset_index(name="num_tx").drop("index", axis=1)
count_blocks.index = range(1, len(count_blocks)+1)
print("TX per mese:")
print(count_blocks)

##################################################################################
#  PER MOSTRARE DISTRIBUZIONE VALORI:
##################################################################################
# count_blocks.plot(style="o-", y="num_tx")
# pyplot.ylabel("Tx")
# pyplot.xlabel("Months")
# pyplot.show()
##################################################################################

# transazioni riguardanti pk che hanno ricevuto transazioni coinbase
pk_uxto = oio.query("sig_id == 0")
pk_uxto = pk_uxto.groupby(
    "pk_id")["value_y"].sum().reset_index(name="total_value")

# conversione da satoshi a bitcoin
pk_uxto["total_value"] = pk_uxto["total_value"]/1e8
##################################################################################
#  PER MOSTRARE DISTRIBUZIONE VALORI:
##################################################################################
# pk_uxto.plot(style="r", y="total_value", x="pk_id")
# pyplot.ylabel("BTC")
# pyplot.show()
##################################################################################

# fees per ogni transazione
print("FEES PER OGNI TRANSAZIONE (BTC): ")
merged_fees["fees"] = merged_fees["fees"]/1e8
print(merged_fees["fees"])
##################################################################################
#  PER MOSTRARE DISTRIBUZIONE VALORI:
##################################################################################
# merged_fees.plot(style="g-", y="fees", use_index=True)
# pyplot.ylabel("Fees (BTC)")
# pyplot.xlabel("Tx")
# pyplot.show()
##################################################################################

# analisi personale: pk che ha speso più soldi

spent = io.query("sig_id != 0").groupby("sig_id")[
    "value"].sum().reset_index(name="spent")
spent["spent"] = spent["spent"]/1e8
max_spent = spent[spent["spent"] == spent["spent"].max()]
print("PK con spesa maggiore: ")
print(max_spent)
