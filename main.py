from datetime import datetime, timedelta

from fastapi import FastAPI
from FinamAPI import get_bars, get_moex_tickers
from google.protobuf.timestamp_pb2 import Timestamp

from scipy.stats import zscore

app = FastAPI()

target_tickers = get_moex_tickers()
print(len(target_tickers), 'MOEX tickers loaded.')

@app.get("/bars")
def get_model(ticker_name: str, timestamp: int):
    start_time = Timestamp(seconds=timestamp)
    end_time = Timestamp(seconds=timestamp + 60 * 60 * 2) # 2 часа
    
    bars_response = get_bars(ticker_name, 'M1', start_time, end_time)
    bars = [
        {
            "open": float(bar.open.value),
            "close": float(bar.close.value),
        }
        for bar in bars_response.bars
    ]
    return {"bars": bars}

def start_monitor():
    import threading
    import time

    def monitor():
        threshold_m1 = 7.0
        threshold_m5 = 5.0
        threshold_m30 = 3.0
        
        calc_time_delta = timedelta(days=3)
        filter_time_delta = timedelta(days=1)

        while True:
            for ticker_name in target_tickers:
                end_time = Timestamp(seconds=int(datetime.timestamp(datetime.now())))
                start_time = Timestamp(seconds=int(datetime.timestamp(datetime.now() - calc_time_delta)))
                
                bars_response_m1 = get_bars(ticker_name, 'M1', start_time, end_time)
                bars_response_m5 = get_bars(ticker_name, 'M5', start_time, end_time)
                bars_response_m30 = get_bars(ticker_name, 'M30', start_time, end_time)
                if bars_response_m1 is None or bars_response_m5 is None or bars_response_m30 is None:
                    print(f"{datetime.now()} - {ticker_name}: No response from server.")
                    continue
                if len(bars_response_m1.bars) > 0 and len(bars_response_m5.bars) > 0 and len(bars_response_m30.bars) > 0:
                    prices_m1 = [float(bar.close.value) - float(bar.open.value) for bar in bars_response_m1.bars]
                    prices_m5 = [float(bar.close.value) - float(bar.open.value) for bar in bars_response_m5.bars]
                    prices_m30 = [float(bar.close.value) - float(bar.open.value) for bar in bars_response_m30.bars]
                    z_scores_m1 = zscore(prices_m1)
                    z_scores_m5 = zscore(prices_m5)
                    z_scores_m30 = zscore(prices_m30)

                    filtered_bars_m1 = [(bars_response_m1.bars[i], z_scores_m1[i]) for i in range(len(bars_response_m1.bars)) if abs(z_scores_m1[i]) > threshold_m1] # выбираем только анамалии
                    filtered_bars_m5 = [(bars_response_m5.bars[i], z_scores_m5[i]) for i in range(len(bars_response_m5.bars)) if abs(z_scores_m5[i]) > threshold_m5] # выбираем только анамалии
                    filtered_bars_m30 = [(bars_response_m30.bars[i], z_scores_m30[i]) for i in range(len(bars_response_m30.bars)) if abs(z_scores_m30[i]) > threshold_m30] # выбираем только анамалии
                    filtered_bars = filter(lambda x: x[0].timestamp.ToDatetime() > datetime.now() - filter_time_delta, filtered_bars) # фильтруем по времени (последние 5 минут)
                    for bar, z in filtered_bars:
                        print(f"{bar.timestamp.ToDatetime() + timedelta(hours=3)} - {ticker_name}: Anomalous bar detected - Open: {bar.open.value}, Close: {bar.close.value}, Z-Score: {z}")
                        if z > 0:
                            print(f"  Potential Buy Signal")
                        else:
                            print(f"  Potential Sell Signal")
                        # TODO: send timestamp
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
