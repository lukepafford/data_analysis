import requests
import pandas as pd
import matplotlib.pyplot as plt

url = 'https://www.animefillerlist.com/shows/one-piece'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
}
text = requests.get(url, headers=headers).text
df = pd.read_html(text)[0]

one_piece = df.groupby('Type').size().sort_values()

one_piece.plot.pie(autopct='%1.1f%%')
plt.title('One Piece Filler')
plt.show()
