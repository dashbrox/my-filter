name: Update Guide

on:
  schedule:
    - cron: "0 */12 * * *"   # cada 12 horas
  workflow_dispatch:

jobs:
  update:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.x"

      - name: Install dependencies
        run: pip install requests

      - name: Run script
        env:
          TMDB_API_KEY: ${{ secrets.TMDB_API_KEY }}
          OMDB_API_KEY: ${{ secrets.OMDB_API_KEY }}
        run: python update_guide.py

      - name: Deploy to gh-pages
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: .   # publica guide_custom.xml desde raíz
          publish_branch: gh-pages
