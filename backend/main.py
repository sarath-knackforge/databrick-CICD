from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# ------------------------------------------------------
# GOOGLE SHEETS API AUTHENTICATION
# ------------------------------------------------------
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Load your service account file (must be included in Render project)
creds = ServiceAccountCredentials.from_json_keyfile_name(
    "service_account.json", SCOPE
)

client = gspread.authorize(creds)

# Your Google Sheet ID (DO NOT USE CSV LINK)
SHEET_ID = "17B_nr58UEikILpOip9Bzy87z8IQrF0H_2XA7qXzlNlE"

# Open FIRST sheet ("Sheet1")
sheet = client.open_by_key(SHEET_ID).sheet1


# ------------------------------------------------------
# APPROVAL ENDPOINT
# ------------------------------------------------------
# @app.get("/approve")
# def approve():
#     try:
#         run_id = request.args.get("run_id")
#         model_name = request.args.get("model_name")
#         model_version = request.args.get("model_version")

#         if not run_id:
#             return jsonify({"error": "Missing run_id"}), 400

#         # Append approval row
#         sheet.append_row([run_id, model_name, model_version, "TRUE"])

#         return jsonify({
#             "status": "SUCCESS",
#             "message": "Approval recorded in Google Sheet",
#             "run_id": run_id,
#             "model_name": model_name,
#             "model_version": model_version
#         })

#     except Exception as e:
#         return jsonify({"status": "ERROR", "error": str(e)}), 500

from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/approve", methods=["GET"])
def approve_model():

    run_id = request.args.get("run_id")
    model_name = request.args.get("model_name")
    model_version = request.args.get("model_version")
    action = request.args.get("action", "APPROVE")  # default approve

    if not run_id or not model_name or not model_version:
        return jsonify({
            "status": "error",
            "message": "Missing required parameters"
        }), 400

    if action == "APPROVE":
        print("✅ MODEL APPROVED")
        print("Run ID:", run_id)
        print("Model Name:", model_name)
        print("Model Version:", model_version)

        # ✅ TODO: Promote model to Production using MLflow
        # mlflow_client.transition_model_version_stage(...)

        return jsonify({
            "status": "success",
            "action": "APPROVED",
            "run_id": run_id,
            "model_name": model_name,
            "model_version": model_version
        })

    elif action == "REJECT":
        print("❌ MODEL REJECTED")
        print("Run ID:", run_id)
        print("Model Name:", model_name)
        print("Model Version:", model_version)

        # ✅ TODO: Log rejection reason / audit trail

        return jsonify({
            "status": "success",
            "action": "REJECTED",
            "run_id": run_id,
            "model_name": model_name,
            "model_version": model_version
        })

    else:
        return jsonify({
            "status": "error",
            "message": "Invalid action"
        }), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)


# ------------------------------------------------------
# HEALTH CHECK
# ------------------------------------------------------
@app.get("/")
def home():
    return "Databricks Approval Backend is running!"


# ------------------------------------------------------
# RUN FLASK (Render uses Gunicorn in production)
# ------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
