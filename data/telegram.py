# pylint: disable=missing-timeout
import requests

from data.util import get_next_business_day

class Telegram:
    """
     This class will help us to send text message to telegram groups
    """
    
    def __init__(self,tg_api_token:str,tg_chat_id:str) -> None:

        self.tg_api_token=tg_api_token
        self.tg_chat_id=tg_chat_id
        
    def send_to_telegram(self, cheapest_records,today,holidays,exclusions):
        # Define column names and calculate maximum symbol length
        columns = ['Symbol', 'Strike', 'Straddle Premium', '%Coverage', 'Current vs prev two months']      
        next_business_day=get_next_business_day(today,holidays,exclusions,days=5)
        # Format message header
        bot_message = f"<b>Scripts for {next_business_day}</b>\n\n"

        # Format column headers
        header = " | ".join(f"<b>{col}</b>" for col in columns)
        bot_message += f"{header}\n{'-' * len(header)}\n"

        # Format record rows
        for rec in cheapest_records:
            row_values = [f"{val:.2f}" if isinstance(val, float) else val for val in (rec[col.lower().replace(' ', '_')] for col in columns)]
            row = " | ".join(str(val) for val in row_values)
            bot_message += f"<code>{row}</code>\n"

        # Format footer with script symbols
        script_symbols = '1!, '.join(rec['symbol'] for rec in cheapest_records)
        bot_message += f"\n<b>Script symbols:</b> <code> {script_symbols}1! </code> \n"

        # Send message to telegram
        self.telegram_bot(bot_message.replace('&','_'))

    def telegram_bot(self,bot_message:str):
        """
        Args:
        bot_message:str
        """
        send_text='https://api.telegram.org/bot'+self.tg_api_token+'/sendMessage?chat_id='+self.tg_chat_id+'&parse_mode=html&text='+bot_message
        response=requests.get(send_text)
        print(response.text)