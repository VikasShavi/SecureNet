from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import lit
from xgboost.spark import SparkXGBClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("example").getOrCreate()

# Replace 'your_file_path.csv' with the actual path to your CSV file
df = spark.read.csv('combined/outputprepro/prepro.csv', header=True, inferSchema=True)
# Define the split ratios
split_ratiosone = [0.7, 0.3]  # 70% in the first DataFrame, 30% in the second DataFrame
# Randomly split the DataFrame
dfsone = df.randomSplit(split_ratiosone, seed=42)  # You can change the seed for reproducibility
# Assign names to the resulting DataFrames
df_train = dfsone[0].withColumn("isVal", lit(False)).withColumn("weight", lit(1.0))
df_train.show(5)
print("-"*75)
split_ratiostwo = [0.5, 0.5]  # 70% in the first DataFrame, 30% in the second DataFrame
dfstwo = dfsone[1].randomSplit(split_ratiostwo, seed=42)
df_val = dfstwo[0].withColumn("isVal", lit(True)).withColumn("weight", lit(1.0))
df_val.show(5)
print("-"*75)
df_combined = df_train.union(df_val)
df_combined.show(5)
print("-"*75)
df_test = dfstwo[1]
df_test.show(5)
print("-"*75)

feature_cols = ['Dur', 'TotPkts', 'TotBytes', 'SrcBytes', 'Proto_tcp', 'Proto_udp', 'Dir_one', 'sTos', 'Proto_others', 'Dir_others']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_combined = assembler.transform(df_combined)
df_test = assembler.transform(df_test)
# df_combined.show(5)
selected_cols = ["StartTime", "features", "Label", "isVal", "weight"]
df_combined = df_combined.select(selected_cols)
selected_cols = ["StartTime", "features", "Label"]
df_test = df_test.select(selected_cols)

xgb_classifier = SparkXGBClassifier(max_depth=5, missing=0.0,
     validation_indicator_col='isVal', weight_col='weight',
     early_stopping_rounds=1, eval_metric='mlogloss')
xgb_clf_model = xgb_classifier.fit(df_combined)
predictions = xgb_clf_model.transform(df_test)
predictions.show()
print("-"*75)
# Evaluate the model using MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(
    labelCol="Label",  # Specify the label column
    predictionCol="prediction",
    metricName="accuracy"  # You can choose other metrics like "f1", "weightedPrecision", etc.
)

accuracy = evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy}")
print("-"*75)
# Specify the path where you want to save the model
model_save_path = "Model/pyspark"

# Save the trained XGBoost model
xgb_clf_model.save(model_save_path)