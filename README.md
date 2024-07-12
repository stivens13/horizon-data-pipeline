# horizon-data-pipeline

# 0. Assumptions
1. A minimalistic data pipeline is being built
2. Currency prices received from CoinGecko (or alternative) are valid for given market as default
3. Every transaction from provided data represents a fulfilled/closing order - meaning that for every `"BUY_ITEMS"` and `"SELL_ITEMS"` event, there's an opposite opening order not present in this data. Hence, every recorded transaction is a fulfilled transaction and can be directly contributed to daily volume indicators

# 1. Installation Process

Install Google Cloud SDK
   e.g.
   `brew install google-cloud-sdk`

Run `gcloud auth login` to authenticate

Run `gcloud components update` to ensure all sdk components are up to date

2. Make sure you have Docker installed and running