{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Run FastAPI (uvicorn)",
      "type": "python",
      "request": "launch",
      "module": "uvicorn",
      "args": ["main:app", "--reload"],
      "jinja": true,
      "console": "integratedTerminal",
      "env": {
        "PYTHONPATH": "${workspaceFolder}"
      }
    }
  ]
}
