# .github/workflows/rebalance.yml

name: Portfolio Rebalance

on:
  schedule:
    - cron: "0 7 * * 1"
    - cron: "0 7 * * 4"
  workflow_dispatch:

permissions:
  contents: write

jobs:
  rebalance:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run rebalance
        env:
          GROQ_API_KEY:   ${{ secrets.GROQ_API_KEY }}
          GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}
        run: python rebalance.py

      - name: Commit results
        run: |
          git config user.name "Portfolio Bot"
          git config user.email "bot@portfolio.com"
          git add data/
          git diff --staged --quiet || git commit -m \
            "Rebalance $(date +%Y-%m-%d): auto-generated"
          git push

      - name: Upload rebalance report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: rebalance-report-${{ github.run_id }}
          path: data/rebalances/
          retention-days: 90

      - name: Send success email
        if: success()
        uses: dawidd6/action-send-mail@v3
        with:
          server_address: smtp.gmail.com
          server_port: 465
          secure: true
          username: ${{ secrets.EMAIL_USERNAME }}
          password: ${{ secrets.EMAIL_PASSWORD }}
          to: ${{ vars.EMAIL_TO }}
          from: Portfolio Autopilot <${{ secrets.EMAIL_USERNAME }}>
          subject: "Portfolio Rebalance OK"
          html_body: file://data/email_report.json
          convert_markdown: false

      - name: Send failure email
        if: failure()
        uses: dawidd6/action-send-mail@v3
        with:
          server_address: smtp.gmail.com
          server_port: 465
          secure: true
          username: ${{ secrets.EMAIL_USERNAME }}
          password: ${{ secrets.EMAIL_PASSWORD }}
          to: ${{ vars.EMAIL_TO }}
          from: Portfolio Autopilot <${{ secrets.EMAIL_USERNAME }}>
          subject: "Portfolio Rebalance FAILED"
          body: |
            El rebalanceo ha fallado.
            Ver logs: https://github.com/${{ github.repository }}/actions/runs/${{ github.run_id }}
