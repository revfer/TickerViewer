from datetime import datetime, timedelta

from fastapi import FastAPI
from FinamAPI import get_bars, get_moex_tickers
from google.protobuf.timestamp_pb2 import Timestamp

from scipy.stats import zscore
import requests

app = FastAPI()

target_tickers = get_moex_tickers()
print(len(target_tickers), 'MOEX tickers loaded.')

@app.get("/bars")
def get_bar(ticker_name: str, time_frame: str = 'M5'):
    start_time = Timestamp(seconds=int(datetime.timestamp(datetime.now() - timedelta(days=2))))
    end_time = Timestamp(seconds=int(datetime.timestamp(datetime.now())))

    bars_response = get_bars(ticker_name, time_frame, start_time, end_time)
    if bars_response is None:
        return {"error": "Failed to retrieve bars"}
    return {"open_value": float(bars_response.bars[-2].open.value),
            "close_value": float(bars_response.bars[-2].close.value),
            "diff_absolute": float(bars_response.bars[-2].close.value) - float(bars_response.bars[-2].open.value),
            "diff_percent": (float(bars_response.bars[-2].close.value) - float(bars_response.bars[-2].open.value)) / float(bars_response.bars[-2].open.value) * 100,
            "ticker_name": ticker_name,}

def start_monitor():
    import threading
    import time

    def monitor():
        while True:
            for ticker_name in target_tickers:
                calc_time_delta = timedelta(days=3)
                filter_time_delta = timedelta(minutes=30)
                end_time = Timestamp(seconds=int(datetime.timestamp(datetime.now())))
                start_time = Timestamp(seconds=int(datetime.timestamp(datetime.now() - calc_time_delta)))
                
                bars_response = get_bars(ticker_name, 'M5', start_time, end_time)
                if bars_response is None:
                    continue
                if len(bars_response.bars) > 0:
                    prices = [float(bar.close.value) - float(bar.open.value) for bar in bars_response.bars]
                    z_scores = zscore(prices)

                    threshold = 7.0
                    filtered_bars = [(bars_response.bars[i], z_scores[i]) for i in range(len(bars_response.bars)) if abs(z_scores[i]) > threshold] # выбираем только анамалии
                    filtered_bars = filter(lambda x: x[0].timestamp.ToDatetime() > datetime.now() - filter_time_delta, filtered_bars) # фильтруем по времени (последние 5 минут)
                    for bar, z in filtered_bars:
                        print(f"{bar.timestamp.ToDatetime() + timedelta(hours=3)} - {ticker_name}: Anomalous bar detected - Open: {bar.open.value}, Close: {bar.close.value}, Z-Score: {z}")
                        if z > 0:
                            print(f"  Potential Buy Signal")
                        else:
                            print(f"  Potential Sell Signal")
                        # TODO: send ticker and (bar.close.value - bar.open.value)
                else:
                    print(f"{datetime.now()} - {ticker_name}: No bars received.")

            time.sleep(60)

    thread = threading.Thread(target=monitor)
    thread.daemon = True
    thread.start()


if __name__ == "__main__":
    start_monitor()
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False
    )
