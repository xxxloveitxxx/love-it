from flask import Flask, render_template
from models import insert_lead, get_all_leads

app = Flask(__name__)

@app.route("/leads")
def leads_page():
    leads = get_all_leads(100)
    return render_template("leads.html", leads=leads)

@app.route("/test-insert")
def test_insert():
    insert_lead("John Doe", "Los Angeles", "Dream Realty", "Sold 12 homes in last 12 months", "https://zillow.com/agent-link")
    return "✅ Test lead inserted!"
