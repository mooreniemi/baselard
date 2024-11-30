from sklearn.preprocessing import FunctionTransformer, OneHotEncoder
import argparse
import numpy as np
from sklearn.tree import DecisionTreeClassifier
from sklearn.datasets import make_classification
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import onnx
from onnx import checker

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Export Decision Tree with optional preprocessing to ONNX.")
parser.add_argument("--no-preprocess", action="store_true", help="Disable preprocessing in the ONNX model.")
parser.add_argument("--f", help="(vs code interactive argument set, ignore)")
args = parser.parse_args()

# Generate synthetic data for a classification task
X, y = make_classification(
    n_samples=100, n_features=4, n_informative=3, n_redundant=1, random_state=42
)

# Define a custom preprocessing function
def custom_transform(x):
    return np.sqrt(np.abs(x))  # Example: Square root of absolute value

# Preprocessing steps (optional, based on argument)
if not args.no_preprocess:
    print("Including preprocessing in the ONNX model...")
    preprocessor = ColumnTransformer(
        transformers=[
            ("custom", FunctionTransformer(custom_transform), [1, 2]),  # Apply custom function
            ("cat", OneHotEncoder(), [0]),  # One-hot encode categorical features
        ]
    )
else:
    print("Preprocessing excluded from the ONNX model.")
    preprocessor = "passthrough"  # Skips preprocessing

# Define the decision tree model
classifier = DecisionTreeClassifier(max_depth=3, random_state=42)

# Combine preprocessing and model into a pipeline
pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("classifier", classifier)])

# Fit the pipeline
pipeline.fit(X, y)

# Path to save the ONNX model
onnx_path = "../data/decision_tree_with_pipeline.onnx"

# Specify the input type for the model
initial_type = [("input", FloatTensorType([None, 4]))]  # Original input shape: [batch_size, 4]

# Attempt to convert the pipeline to ONNX
try:
    onnx_model = convert_sklearn(
        pipeline,
        initial_types=initial_type,
        target_opset=9,  # Ensure compatibility with ONNX Runtime
        options={"zipmap": False},  # Disable zipmap for compatibility
    )
    # Save the ONNX model to a file
    with open(onnx_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f"Pipeline with decision tree model has been converted to ONNX and saved as {onnx_path}")

    # Validate the ONNX model
    model = onnx.load(onnx_path)
    checker.check_model(model)
    print("The ONNX model is valid.")
except Exception as e:
    print(f"Conversion to ONNX failed: {e}")