from datetime import datetime, timedelta

from FinamPy import FinamPy
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.interval_pb2 import Interval
from FinamPy.grpc.marketdata.marketdata_service_pb2 import BarsRequest, BarsResponse, TimeFrame
from FinamPy.grpc.assets.assets_service_pb2 import AssetsRequest, AssetsResponse


class FinamAPI:
    def __init__(self):
        self.fp_provider = FinamPy()

    def __del__(self):
        self.fp_provider.close_channel()

    def get_bars(self, symbol: str, timeframe: str, start_time: Timestamp, end_time: Timestamp) -> BarsResponse:
        time_frames = {
            'M1': TimeFrame.TIME_FRAME_M1,
            'M5': TimeFrame.TIME_FRAME_M5,
            'M15': TimeFrame.TIME_FRAME_M15,
            'M30': TimeFrame.TIME_FRAME_M30,
            'H1': TimeFrame.TIME_FRAME_H1,
            'H2': TimeFrame.TIME_FRAME_H2,
            'H4': TimeFrame.TIME_FRAME_H4,
            'H8': TimeFrame.TIME_FRAME_H8,
            'D': TimeFrame.TIME_FRAME_D,
            'W': TimeFrame.TIME_FRAME_W,
            'MN': TimeFrame.TIME_FRAME_MN,
            'QR': TimeFrame.TIME_FRAME_QR
        }
        bars_response: BarsResponse = self.fp_provider.call_function(
            self.fp_provider.marketdata_stub.Bars,
            BarsRequest(symbol=symbol, timeframe=time_frames.get(timeframe, TimeFrame.TIME_FRAME_M1), interval=Interval(start_time=start_time, end_time=end_time))
        )
        return bars_response

    def get_tickers_names(self):
        assets: AssetsResponse = self.fp_provider.call_function(self.fp_provider.assets_stub.Assets, AssetsRequest())
        tickers = [asset.symbol for asset in assets.assets]
        return tickers

    def get_moex_tickers(self):
        tickers = self.get_tickers_names()
        moex_tickers = [ticker for ticker in tickers if ticker.endswith('@MISX')]
        return moex_tickers

if __name__ == '__main__':
    #time_frame = TimeFrame.TIME_FRAME_M1
    #time_delta = timedelta(minutes=30)
    #end_date = datetime.now()
    #start_date = end_date - time_delta
    #start_time = Timestamp(seconds=int(datetime.timestamp(start_date)))
    #end_time = Timestamp(seconds=int(datetime.timestamp(end_date)))
    #bars_response: BarsResponse = get_bars('SBER@MISX', time_frame, start_time, end_time)
    #print(len(bars_response.bars))
    #for bar in bars_response.bars:
    #    print(bar)
    api = FinamAPI()
    tickers = api.get_moex_tickers()
    print(tickers)
