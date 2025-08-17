import os
from flask import Flask, render_template, jsonify, request
from models import save_leads, get_all_leads
from scraper import scrape_zillow_agents

app = Flask(__name__)

@app.get("/")
def index():
    rows = get_all_leads(limit=200)
    return render_template("leads.html", leads=rows)

@app.post("/scrape")
def scrape():
    city = request.args.get("city", "new-york-ny")
    limit = int(request.args.get("limit", "10"))
    leads = scrape_zillow_agents(city=city, limit=limit)
    inserted = save_leads(leads)
    return jsonify({"ok": True, "attempted": len(leads), "inserted": inserted})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
