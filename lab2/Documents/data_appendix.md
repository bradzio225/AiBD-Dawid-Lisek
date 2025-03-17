# Przewodnik po plikach
Wszystkie pliki znajdują się w folderze Analysis_Data. Opis poszczególnych plików:

# weather_cleaned.txt
- plik tekstowy zawierający dane pogodowe po oczyszczeniu ich z flag 'I' oraz 'S'.

# tidy_weather.csv 
- plik csv, który powstał po obróbce pierwotnego pliku tekstowego. Zostały w nim uporządkowane oraz opisane wszystkie zmienne wraz z danymi. Zmienne:

id - identyfikator urządzenia pomiarowego

year, month - rok oraz miesiąc pomiaru

element - rodzaj wykonywanego pomiaru

d1-d31 - dzień w którym został wykonany pomiar

# weather_temp_by_days.csv 
- plik csv, który zawiera dane o temperaturze maksymalnej oraz minimalnej we wszystkich pomiarach w 2010 roku. Zmienne:

Date - data wykonanego pomiaru
T_MIN - minimalna temperatura w trakcie dnia
T_MAX - maksymalna temperatura w trakcie dnia
