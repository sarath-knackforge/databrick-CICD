from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

app = Flask(__name__)

google_creds_json = os.getenv("SERVICE_ACCOUNT_JSON")  # ✅ MATCHES RENDER
print("✅ ENV VAR FOUND:", bool(os.getenv("SERVICE_ACCOUNT_JSON")))
# ------------------------------------------------------
# GOOGLE SHEETS API AUTHENTICATION
# ------------------------------------------------------
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

if not google_creds_json:
    raise RuntimeError("❌ SERVICE_ACCOUNT_JSON env var is NOT set in Render")

creds_dict = json.loads(google_creds_json)

# Load your service account file (must be included in Render project)
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    creds_dict, SCOPE
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

# ------------------------------------------------------
# APPROVAL ENDPOINT — FIXED
# ------------------------------------------------------
@app.get("/approve")
def approve():
    try:
        run_id = request.args.get("run_id")
        model_name = request.args.get("model_name")
        model_version = request.args.get("model_version")
        action = request.args.get("action", "APPROVE")  # ✅ default approve

        if not run_id:
            return jsonify({"error": "Missing run_id"}), 400

        # ✅ ✅ MAP ACTION → SHEET VALUE
        if action == "APPROVE":
            approved_flag = "TRUE"
        elif action == "REJECT":
            approved_flag = "FALSE"
        else:
            return jsonify({"error": "Invalid action"}), 400

        # ✅ READ ALL ROWS
        records = sheet.get_all_records()

        updated = False

        # ✅ UPDATE EXISTING ROW IF FOUND
        for idx, row in enumerate(records, start=2):  # row 1 is header
            if (
                str(row["run_id"]) == str(run_id)
                and str(row["model_name"]) == str(model_name)
                and str(row["model_version"]) == str(model_version)
            ):
                sheet.update_cell(idx, 4, approved_flag)  # ✅ Column D = approved_flag
                updated = True
                break

        # ✅ IF NOT FOUND → APPEND NEW ROW
        if not updated:
            sheet.append_row([
                run_id,
                model_name,
                model_version,
                approved_flag
            ])

        return jsonify({
            "status": "SUCCESS",
            "message": "Approval updated in Google Sheet",
            "action": action,
            "approved_flag": approved_flag,
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
