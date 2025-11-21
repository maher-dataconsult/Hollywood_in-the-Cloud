🎬 From 1902 to 2024: I Built a Cloud Pipeline to Analyze 122 Years of Hollywood Data

I'm excited to share my latest data engineering project: "Hollywood in the Cloud" – a fully automated, serverless pipeline that processes over a century of US Box Office data.

🎯 THE CHALLENGE:
How do you turn 100+ years of film industry data into actionable insights for production studios and distributors?

⚙️ THE SOLUTION:
I built an end-to-end AWS data pipeline that automatically:
• Ingests data via Lambda functions
• Stores raw files in S3 Data Lake
• Transforms data with Glue crawlers
• Queries with Athena
• Visualizes insights in Power BI

🔧 TECHNICAL WINS:
✅ Overcame Lambda timeout constraints (3s → 15min)
✅ Architected S3 structure for optimal crawler performance
✅ Configured granular IAM roles for security
✅ Connected Power BI to Athena via ODBC for real-time dashboards

📊 FASCINATING INSIGHTS:
🎭 Drama & Comedy lead in VOLUME, but Adventure & Action win in POPULARITY
📅 70% of releases target Q4 (Oct-Nov) for holiday audiences and awards season
🌍 French films dominate foreign language cinema (739 films – nearly 2x Italian/Japanese)

💡 KEY TAKEAWAY:
Serverless architecture isn't just about cost savings – it's about building scalable, maintainable solutions that transform raw data into strategic business intelligence.

The entire pipeline runs automatically, from API to dashboard, with zero server management.

🔗 Full technical breakdown and code available on GitHub [link in comments]

What's your experience with serverless data pipelines? I'd love to hear your thoughts!

#DataEngineering #AWS #CloudComputing #ServerlessArchitecture #DataAnalytics #ETL #BigData #PowerBI #DataScience #BoxOffice #FilmIndustry
