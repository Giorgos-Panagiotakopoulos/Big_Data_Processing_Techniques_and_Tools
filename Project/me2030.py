import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import time
from pyspark.sql.functions import monotonically_increasing_id, col
import pyspark.sql.functions as func
import neighborcells as ns

spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "8g").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-14.0.2"
os.environ["SPARK_HOME"] = "C:\Program Files\spark-3.1.1-bin-hadoop2.7"


# Τα datasets πρέπει να έχουν την εξής γραμμογράφηση:
# 1η στήλη: το id του σημείου
# 2η στήλη: η συντεταγμένη x (longitude) του σημείου
# 3η στήλη: η συντεταγμένη y (latitude) του σημείου
# Δεν ενδιαφέρει ο τίτλος της στήλης. Η εφαρμογή μετονομάζει
# τις αντίστοιχες στήλες σε id, x και y
# ώστε να λειτουργούν τα sparkSQL queries
# Διαβάζει το πρώτο dataset και δημιουργεί dataframe.
# Το dataset επίσης μπορεί να περιέχει οποιονδήποτε αριθμό στηλών
# πέρα από τις 3 αρχικές, οι οποίες πρέπει να έχουν τη σειρά
# που αναφέρθηκε πιο πάνω (id, lon, lat)

# Ξεκινάμε να μετράμε το χρόνο που χρειάζεται το πρόγραμμα
# για να δημιουργήσει τα partitions
start = time.time()

input = sqlContext.read.csv ("C:\\backups\\SetA.csv", sep=';', header=True, inferSchema=True)
columns = input.columns
# Μετονομασία των τριών πρώτων στηλών του dataframe που προέκυψε
# από το διάβασμα του .csv αρχείου σε id, x, y
# με χρήση της μεθόδου .withColumnRenamed()
inputA = input.withColumnRenamed(columns[0], 'id').withColumnRenamed(columns[1], 'x').withColumnRenamed(columns[2], 'y')
# Δημιουργία μιας SQL view από το dataframe
inputA.createOrReplaceTempView("set_a")
# Υπολογισμός των ελάχιστων και μέγιστων συντεταγμένων
# των σημείων του πρώτου dataset με χρήση sparkSQL
result = sqlContext.sql("SELECT max(x) AS maxAx FROM set_a")
for item in result.collect():
    maxAx = item["maxAx"]
result = sqlContext.sql("SELECT max(y) AS maxAy FROM set_a")
for item in result.collect():
    maxAy = item["maxAy"]
result = sqlContext.sql("SELECT min(x) AS minAx FROM set_a")
for item in result.collect():
    minAx = item["minAx"]
result = sqlContext.sql("SELECT min(y) AS minAy FROM set_a")
for item in result.collect():
    minAy = item["minAy"]

sqlContext.sql("SELECT min(x) AS minAx, min(y) AS minAy, max(x) AS maxAx, max(y) AS maxAy FROM set_a").show()

# Διάβασμα του δεύτερου dataset
# Το dataset πρέπει να έχει την εξής γραμμογράφηση:
# 1η στήλη: το id του σημείου
# 2η στήλη: η συντεταγμένη x (longitude) του σημείου
# 3η στήλη: η συντεταγμένη y (latitude) του σημείου
input = sqlContext.read.csv ("C:\\backups\\SetB.csv", sep=';', header=True, inferSchema=True)
columns = input.columns
# Μετονομασία των τριών πρώτων στηλών του dataframe που προέκυψε
# από το διάβασμα του .csv αρχείου σε id, x, y
# με χρήση της μεθόδου .withColumnRenamed()
inputB = input.withColumnRenamed(columns[0], 'id').withColumnRenamed(columns[1], 'x').withColumnRenamed(columns[2], 'y')
# Δημιουργία μιας SQL view από το dataframe
inputB.createOrReplaceTempView("set_b")
# Υπολογισμός των ελάχιστων και μέγιστων συντεταγμένων
# των σημείων του πρώτου dataset με χρήση sparkSQL
result = sqlContext.sql("SELECT max(x) AS maxBx FROM set_b")
for item in result.collect():
    maxBx = item["maxBx"]
result = sqlContext.sql("SELECT max(y) AS maxBy FROM set_b")
for item in result.collect():
    maxBy = item["maxBy"]
result = sqlContext.sql("SELECT min(x) AS minBx FROM set_b")
for item in result.collect():
    minBx = item["minBx"]
result = sqlContext.sql("SELECT min(y) AS minBy FROM set_b")
for item in result.collect():
    minBy = item["minBy"]

minX = min(minAx, minBx)
minY = min(minAy, minBy)
maxX = max(maxAx, maxBx)
maxY = max(maxAy, maxBy)
sqlContext.sql("SELECT min(x) AS maxBx, min(y) AS maxBy, max(x) AS maxBx, max(y) AS maxBy FROM set_b").show()

# Δηλώνεται ο αριθμός των πλαισίων (partitions)
# κατά τον οριζόντιο και κατακόρυφο άξονα
splitsX = 5
splitsY = 5

# ΥΠΟΛΟΓΙΣΜΟΣ ΤΩΝ ΔΙΑΣΤΑΣΕΩΝ ΚΑΘΕ PARTITION ΣΤΑ
# ΟΠΟΙΑ ΧΩΡΙΣΤΗΚΕ Η ΕΠΙΦΑΝΕΙΑ ΤΩΝ ΣΗΜΕΙΩΝ
# ----------------------------------------------------------------
# W (κεφαλαίο) είναι το πλάτος του παραλληλογράμμου που περικλείει
# όλα τα σημεία του συνόλου Α
W = maxX - minX
# Η (κεφαλαίο) είναι το ύψος του παραλληλογράμμου που περικλείει
# όλα τα σημεία του συνόλου Α
H = maxY - minY
# w (μικρό) είναι το πλάτος κάθε παραλληλογράμμου, στα οποία
# διαιρείται το μεγάλο παραλληλόγραμμο
w = W / splitsX
# h (μικρό) είναι το ύψος κάθε παραλληλογράμμου, στα οποία
# διαιρείται το μεγάλο παραλληλόγραμμο
h = H / splitsY

# ΠΡΟΣΘΗΚΗ ΣΕ ΚΑΘΕ ΕΝΑ ΑΠΟ ΤΑ ΔΥΟ DATAFRAMES ΜΙΑΣ ΣΤΗΛΗΣ ΜΕ ΤΟ ΟΝΟΜΑ 'bin'
# ΣΤΗΝ ΟΠΟΊΑ ΚΑΤΑΧΩΡΕΊΤΑΙ Ο ΑΡΙΘΜΟΣ ΤΟΥ ΠΛΑΙΣΙΟΥ ΣΤΟ ΟΠΟΙΟ ΑΝΗΚΕΙ ΧΩΡΙΚΑ
# ΤΟ ΚΑΘΕ ΣΗΜΕΙΟ ΤΟΥ.
# Η ΠΡΑΞΗ ΟΔΗΓΕΙ ΣΤΗ ΔΗΜΙΟΥΡΓΙΑ ΝΕΩΝ DATAFRAMES, ΔΕΔΟΜΕΝΟΥ ΟΤΙ Η ΔΟΜΗ DATAFRAME
# EINAI IMMUTABLE
# Ο υπολογισμός του αριθμού του πλαισίου (partition) στο οποίο ανήκει κάθε σημείο
# γίνεται με τον τύπο:
# Απόσταση Υ του σημείου από το ελάχιστο Y / ύψος πλαισίου * Πλήθος πλαισίων οριζόντια
# +
# Απόσταση Χ του σημείου από το ελάχιστο Χ / πλάτος του πλαισίου
newInputA = inputA.withColumn("bin", func.round((((col("y") - minY) / h)-1) * splitsX + (col("x")-minX) / w, 0))
newInputA = newInputA.withColumn("dataset", func.lit("A"))
newInputB = inputB.withColumn("bin", func.round((((col("y") - minY) / h)-1) * splitsX + (col("x")-minX) / w, 0))
newInputB = newInputB.withColumn("dataset", func.lit("B"))

newInputA.show(20)
newInputB.show(20)
# Δημιουργία  SQL view από τα δύο
# νέα dataframe
newInputA.createOrReplaceTempView("set_a")
newInputB.createOrReplaceTempView("set_b")

# Κώδικας ελέγχου των views που δημιουργήθηκαν
points = sqlContext.sql ("SELECT set_a.id, set_a.x, set_a.y, set_a.bin FROM set_a LIMIT 80")
print(points.collect())
points = sqlContext.sql ("SELECT set_b.id, set_b.x, set_b.y, set_b.bin FROM set_b LIMIT 80")
print(points.collect())

# Ορισμός της επιθυμητής απόστασης μεταξύ των σημείων
# που θα συμπεριληφθούν στο αποτέλεσμα
theta = 5

# Είναι ο συνολικός αριθμός partitions που πρκύπτει από το γινόμενο
# των κελιών (bins) οριζόντια και κατακόρυφα
noOfCells = splitsY * splitsX

# Η λίστα copied_points
copied_points = []

# Για κάθε ένα bin εκτελούμε μία sparkSQL η οποία ψάχνει να βρει ποια στοιχεία
# των γειτονικών του bins βρίσκονται σε απόσταση μικρότερη ή ίση του theta
for bin in range (0, noOfCells):
    strSQL =   "SELECT  DISTINCT set_a.id, set_a.x, set_a.y, set_a.bin, " \
               "        SQRT(POWER(set_a.y-set_b.y, 2) + POWER(set_a.x-set_b.x, 2)) AS Distance " \
               "FROM    set_a, set_b " \
               "WHERE   set_b.bin = " + str(bin) + " AND set_a.bin IN (" + ns.neighborCellsStr(splitsY, splitsX, bin) + ") AND " \
                        "SQRT(POWER(set_a.y-set_b.y, 2) + POWER(set_a.x-set_b.x, 2)) < " + str(theta)
    points = sqlContext.sql(strSQL)
    print ("Points that are nearer to theta from bin " + str(bin))
    for item in points.collect():
        print(item)
        copy_point = (item['id'], item['x'], item['y'], bin, 'A')
        copied_points.append(copy_point)
    print ("==============================================================================")

columns = ['id', 'x', 'y', 'bin', 'dataset']
if len(copied_points) > 0:
    df = spark.createDataFrame(copied_points, columns)
    all = newInputA.union(df)
    all = all.union(newInputB)
else:
    print ("No points copied to neighbouring bins")
    all = newInputA
    all = all.union(newInputB)

# Δημιουργεί partitions βάσει του αριθμού του
# πλαισίου στο οποίο ανήκει κάθε σημείο
all.repartition(1).write.partitionBy('bin')
all.createOrReplaceTempView("set_a")

end = time.time()
running_time = end - start
print ("Τime to create partitions: " + str(running_time) + " sec")

print ("Starting query...")
start = time.time()
points = sqlContext.sql(   "SELECT A.id, B.id \
                            FROM \
                            ( \
                            SELECT  set_a.id AS id, set_a.x AS x, set_a.y as y \
                            FROM set_a \
                            WHERE dataset = 'A') AS A \
                            INNER JOIN \
                            (SELECT set_a.id, set_a.x AS x, set_a.y as y \
                            FROM set_a \
                            WHERE dataset = 'B') AS B \
                            ON SQRT(POWER(B.y-A.y, 2) + POWER(B.x-A.x, 2)) <=" + str(theta))
end = time.time()
running_time = end - start
print ("Τime to find nearest neighbours: " + str(running_time) + " sec")
#print ("Located " + str(points.collect().count("id")) + " points.")
#for item in points.collect():
    #print(item)