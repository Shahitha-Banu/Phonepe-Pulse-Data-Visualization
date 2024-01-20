# Phonepe-Pulse-Data-Visualization

This project intends to provide users to visualize the Github PhonePe Repository details of India for the period from 2018 - 2023 on various metrics.

phonepe_plus.py
  Extraction of cloned Repositoty data to PostgreSQL
phonepe_plot.py
  Visualization of extracted data in the form of plots in streamlit application on various insights.

Libraries to be imported:
1.os

2.json

3.plotly.express

4.streamlit

5.psycopg2

6.pandas

Approach:
1. Data extraction: Clone the Github using scripting to fetch the data from the Phonepe pulse Github repository and store in JSON format.

2. Data transformation: Transform the JSON file to SQL table and store it in POSTGRESQL database.

3. Dashboard creation: Using the Streamlit and Plotly libraries an interactive and visually appealing dashboard has been created.

Work flow:
The user can navigate through multiple options provide to view the data graphically
