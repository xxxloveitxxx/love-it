# app.py
from flask import Flask, render_template, jsonify
from scraper import scrape_real_estate_agents
from models import save_leads, get_all_leads

app = Flask(__name__)

@app.route("/")
def index():
    leads = get_all_leads()
    return render_template("leads.html", leads=leads)

@app.route("/scrape")
def scrape():
    leads = scrape_real_estate_agents(limit=5)
    save_leads(leads)
    return jsonify({"status": "ok", "new_leads": len(leads)})

if __name__ == "__main__":
    app.run(debug=True)
