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
@app.get("/approve")
def approve():
    try:
        run_id = request.args.get("run_id")
        model_name = request.args.get("model_name")
        model_version = request.args.get("model_version")

        if not run_id:
            return jsonify({"error": "Missing run_id"}), 400

        # Append approval row
        sheet.append_row([run_id, model_name, model_version, "TRUE"])

        return jsonify({
            "status": "SUCCESS",
            "message": "Approval recorded in Google Sheet",
            "run_id": run_id,
            "model_name": model_name,
            "model_version": model_version
        })

    except Exception as e:
        return jsonify({"status": "ERROR", "error": str(e)}), 500


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
