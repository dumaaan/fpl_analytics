"""Entry point for Streamlit Community Cloud.

Community Cloud adds the entry file's folder to the import path, so this file
sits at the repository root. That makes `dashboard` and `fpl_analytics`
importable without installing the project.
"""

from dashboard.app import main

main()
