## .gitignore

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
venv/
ENV/
env/
.venv

# IDE
.vscode/
.idea/
*.swp
*.swo
.DS_Store

# Simulator Generated Data
Server*/
server*/

# Logs
*.log
simulator.log
growth_simulator.log
processing_log.txt

# Temporary files
*.tmp
*.bak
.~*

# Optional - keep generated data in repo
# Comment these lines if you want to include sample data
# Server*/growth_data/snapshots/
# Server*/growth_data/autogrowth_events/
# Server*/growth_data/server_state.json