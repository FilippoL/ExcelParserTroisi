import pandas as pd
import os
import time
import pyautogui
import html
import json
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import textwrap as twp
from tg_tqdm import tg_tqdm
import copy
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from telegram.constants import ParseMode
from pathlib import Path
import requests
import logging
import traceback

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# Set LaTeX rendering for matplotlib
plt.rcParams['text.usetex'] = True
pd.set_option('future.no_silent_downcasting', True)

# Mapping alphabet characters to indices
alphabet_to_index = {chr(i): i - ord('A') for i in range(ord('A'), ord('Z') + 1)}

def filter_cinema(df, cinema_name):
    return df[df["Cinema"].str.fullmatch(cinema_name)]

def clean_file(file_in):
    os.startfile(file_in, 'edit')
    time.sleep(5)
    pyautogui.hotkey('ctrl', 's')
    pyautogui.hotkey('alt', 'f4')

async def process_xlsx_to_excel(update: Update, context):
    # Predefined data
    cities = {'Roma':['Troisi', 'Barberini', 'Quattro Fontane', 'Farnese', 'Intrastevere', 'Nuovo Sacher', 'Greenwich'], 'Milano':['Beltrade'], 'Bologna':['Cinema Modernissimo'], 'Trento':['Roma']}

    column_aggregated_labels = ['Sala', 'Incassi totali', 'Presenze totali', 'Prezzo medio']
    column_detailed_labels = ['Film', 'Incassi', 'Presenze', 'Prezzo medio']

    print("Processing files sent...")

    # List Excel files in the current directory
    results = [each for each in os.listdir(".") if each.endswith('.xlsx')]

    if len(results) > 0:
        dfs = []

        # Process each Excel file
        for file in results:
            full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file)

            try:
                df = pd.read_excel(full_path)
            except ValueError as e:
                clean_file(full_path)
                df = pd.read_excel(full_path)

            # Clean up DataFrame
            df.columns = list(df.iloc[1])
            df = df[2:-2].reset_index(drop=True)
            dfs.append(df)

        # Merge all DataFrames
        df_merged = pd.concat(dfs, ignore_index=True, sort=False)
        df_merged.fillna(0, inplace=True)
        df_merged["Città"] = df_merged["Città"].str.title()
        df_merged["Cinema"] = df_merged["Cinema"].str.title()
        df_merged["Titolo Film"] = df_merged["Titolo Film"].str.title()
        df_merged.drop_duplicates(inplace=True)

        df_merged = df_merged.loc[df_merged['Città'].isin(list(cities.keys()))]

        # Filter cinema names and create a dictionary of DataFrames for each cinema
        names = []
        [names.extend(c) for c in cities.values()]
        cinema_names = list(set(names).intersection(list(df_merged["Cinema"])))
        df_cinemas = {name: filter_cinema(df_merged, name) for name in cinema_names}

        # Extract relevant columns
        columns_incasso = [col for col in df_merged.columns if "Incass" in col.split()[0]]
        columns_presenze = [col for col in df_merged.columns if "Presenz" in col]

        # Filter cities and create a dictionary of DataFrames for each city
        filtered_cities = list(set(cities.keys()).intersection(list(df_merged["Città"])))
        df_cinema_city = {city: df_merged.loc[df_merged['Città'] == city] for city in filtered_cities}

        # Get current datetime for file naming
        current_datetime = datetime.now()
        formatted_date = current_datetime.strftime("%d_%m_%Y")
        full_name = f'Analisi_{formatted_date}.xlsx'

        writer = pd.ExcelWriter(full_name, engine='xlsxwriter')

        table_data = {}

        to_remove = []

        # Iterate over each city
        for city in filtered_cities:
            curr_df = df_cinema_city[city]
            curr_keys = list(set(names).intersection(list(curr_df["Cinema"])))

            if len(curr_keys) == 0:
                print(f"No matching cinema for {city}")
                to_remove.append(city)
                continue

            dati_per_cinema = {name: [round(sum([sum(df_cinemas[name][key]) for key in columns_incasso]), 2)] for name in curr_keys}
            {dati_per_cinema[name].append(round(sum([sum(df_cinemas[name][key].apply(pd.to_numeric)) for key in columns_presenze]), 2)) for name in curr_keys}
            {dati_per_cinema[key].append(round(value[0] / value[1], 2)) for key, value in dati_per_cinema.items()}

            table_data[city] = [[key] + dati_per_cinema[key] for key in curr_keys]

        # Remove cities with no matching cinema
        [filtered_cities.remove(val) for val in to_remove]

        rome_table_data = False

        if "Roma" in list(table_data.keys()):
            # Sort and create a table for Rome only
            rome_table_data = copy.deepcopy(table_data["Roma"])
            rome_table_data.sort(key=lambda x: (-x[2]))
            rome_df = pd.DataFrame(rome_table_data, columns=column_aggregated_labels)
            rome_df.to_excel(writer, sheet_name='Roma', index=False)

            # Create a table for other cities
            troisi_data = [val for val in rome_table_data if val[0] == "Troisi"]
            troisi_data[0].insert(0, "Roma")
            troisi_data = troisi_data[0]
            other_cities_data = [[[c] + el for el in table_data[c]] for c in filtered_cities if c != "Roma"]
            other_cities_data = [i for row in other_cities_data for i in row]
            if rome_table_data:
                other_cities_data.append(troisi_data)
            other_cities_df = pd.DataFrame(other_cities_data, columns=['Città'] + column_aggregated_labels)
            other_cities_df.to_excel(writer, sheet_name='Altre Città', index=False)

        # Create detailed analysis for each cinema
        if "Roma" in list(table_data.keys()):
            sorted_cinema_names = [val[0] for val in table_data["Roma"]]
            sorted_cinema_names.extend([table_data[key][0][0] for key in table_data.keys() if key != "Roma"])
            df_cinemas["Roma"] = df_cinemas["Roma"][df_cinemas["Roma"]["Città"] == "Trento"]
        else:
            sorted_cinema_names = [table_data[key][0][0] for key in table_data.keys()]

        for cinema in sorted_cinema_names:
            table_data = []
            movies = df_cinemas[cinema]["Titolo Film"]
            incassi_totali = 0
            presenze_totali = 0

            for i, movie in enumerate(movies):
                target_row = df_cinemas[cinema].loc[df_merged['Titolo Film'] == movie]
                incassi_film = target_row[columns_incasso].apply(pd.to_numeric).sum().sum()
                presenze_film = target_row[columns_presenze].apply(pd.to_numeric).sum().sum()
                costo_medio = round(incassi_film / presenze_film, 2)
                table_data.append([twp.fill(movie, 40), round(incassi_film, 2), presenze_film, costo_medio])
                incassi_totali += incassi_film
                presenze_totali += presenze_film

            table_data.sort(key=lambda x: (-x[2]))
            table_data.append(['Totale', round(incassi_totali, 2), presenze_totali, round(incassi_totali / presenze_totali, 2)])
            detailed_df = pd.DataFrame(table_data, columns=column_detailed_labels)
            detailed_df.to_excel(writer, sheet_name=cinema, index=False)

        writer.save()

        await context.bot.send_document(update.message.chat_id, document=open(full_name, 'rb'))
    else:
        await update.message.reply_text("Non ho trovato file excel (.xlsx), fai l'upload dei file prima.")

    current_directory = os.getcwd()

    # List all files in the current directory
    files = os.listdir(current_directory)

    # Iterate through the files and remove each one, excluding those ending with ".py"
    for file in files:
        file_path = os.path.join(current_directory, file)
        if os.path.isfile(file_path) and not file.endswith(".py"):
            os.remove(file_path)



async def process_xlsx(update: Update, context):
    # Predefined data
    cities = {'Roma':['Troisi', 'Barberini', 'Quattro Fontane', 'Farnese', 'Intrastevere', 'Nuovo Sacher', 'Greenwich'], 'Milano':['Beltrade'], 'Bologna':['Cinema Modernissimo'], 'Trento':['Roma']}

    column_aggregated_labels = [r'\textbf{Sala}', r'\textbf{Incassi totali}', r'\textbf{Presenze totali}', r'\textbf{Prezzo medio}']
    column_detailed_labels = [r'\textbf{Film}', r'\textbf{Incassi}', r'\textbf{Presenze}', r'\textbf{Prezzo medio}']

    print("Processing files sent...")

    # List Excel files in the current directory
    results = [each for each in os.listdir(".") if each.endswith('.xlsx')]

    if len(results) > 0:
        dfs = []

        # Process each Excel file
        for file in results:
            full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file)

            try:
                df = pd.read_excel(full_path)
            except ValueError as e:
                clean_file(full_path)
                df = pd.read_excel(full_path)

            # Clean up DataFrame
            df.columns = list(df.iloc[1])
            df = df[2:-2].reset_index(drop=True)
            dfs.append(df)

        # Merge all DataFrames
        df_merged = pd.concat(dfs, ignore_index=True, sort=False)
        df_merged.fillna(0, inplace=True)
        df_merged["Città"] = df_merged["Città"].str.title()
        df_merged["Cinema"] = df_merged["Cinema"].str.title()
        df_merged["Titolo Film"] = df_merged["Titolo Film"].str.title()
        df_merged.drop_duplicates(inplace=True)

        df_merged = df_merged.loc[df_merged['Città'].isin(list(cities.keys()))]
       
        # Filter cinema names and create a dictionary of DataFrames for each cinema
        names = []
        [names.extend(c) for c in cities.values()]
        cinema_names = list(set(names).intersection(list(df_merged["Cinema"])))
        df_cinemas = {name: filter_cinema(df_merged, name) for name in cinema_names}

        # Extract relevant columns
        columns_incasso = [col for col in df_merged.columns if "Incass" in col.split()[0]]
        columns_presenze = [col for col in df_merged.columns if "Presenz" in col]

        # Filter cities and create a dictionary of DataFrames for each city
        filtered_cities = list(set(cities.keys()).intersection(list(df_merged["Città"])))
        df_cinema_city = {city: df_merged.loc[df_merged['Città'] == city] for city in filtered_cities}

        # Get current datetime for file naming
        current_datetime = datetime.now()
        formatted_date = current_datetime.strftime("%d_%m_%Y")
        full_name = f'Analisi_{formatted_date}.pdf'

        # Generate PDF file with analysis
        with PdfPages(full_name) as pdf:
            table_data = {}

            to_remove = []

            # Iterate over each city
            for city in filtered_cities:
                curr_df = df_cinema_city[city]
                curr_keys = list(set(names).intersection(list(curr_df["Cinema"])))

                if len(curr_keys) == 0:
                    print(f"No matching cinema for {city}")
                    to_remove.append(city)
                    continue

                dati_per_cinema = {name: [round(sum([sum(df_cinemas[name][key]) for key in columns_incasso]), 2)] for name in curr_keys}
                {dati_per_cinema[name].append(round(sum([sum(df_cinemas[name][key].apply(pd.to_numeric)) for key in columns_presenze]), 2)) for name in curr_keys}
                {dati_per_cinema[key].append(round(value[0] / value[1], 2)) for key, value in dati_per_cinema.items()}

                fig = plt.figure(dpi=100)
                table_data[city] = [[key] + dati_per_cinema[key] for key in curr_keys]

            # Remove cities with no matching cinema
            [filtered_cities.remove(val) for val in to_remove]

            rome_table_data = False

            if "Roma" in list(table_data.keys()):
                # Sort and create a table for Rome only
                rome_table_data = copy.deepcopy(table_data["Roma"])
                rome_table_data.sort(key=lambda x: (-x[2]))
                table = plt.table(cellText=rome_table_data, loc='center', colLabels=column_aggregated_labels, cellLoc="center")
                fig.suptitle(f"Totale Competitor Roma", fontsize=10)
                plt.axis('off')
                table.auto_set_font_size(False)
                table.set_fontsize(6)
                table.scale(1.2, 1.2)
                for i in range(len(column_aggregated_labels)):
                    table[0, i].set_facecolor('#add8e6')

                pdf.savefig(fig)
                plt.clf()

                # Create a table for other cities
                troisi_data = [val for val in rome_table_data if val[0] == "Troisi"]
                troisi_data[0].insert(0, "Roma")
                troisi_data = troisi_data[0]

            other_cities_data = [[[c] + el for el in table_data[c]] for c in filtered_cities if c != "Roma"]
            other_cities_data = [i for row in other_cities_data for i in row]
            if rome_table_data:
                other_cities_data.append(troisi_data)
            other_cities_data.sort(key=lambda x: (-x[3]))

            table = plt.table(cellText=other_cities_data, loc='center', colLabels=['\\textbf{Città}'] + column_aggregated_labels, cellLoc="center")
            fig.suptitle(f"Totale Competitor Nazionali", fontsize=10)
            plt.axis('off')
            table.auto_set_font_size(False)
            table.set_fontsize(6)
            table.scale(1.2, 1.2)

            for i in range(len(column_aggregated_labels) + 1):
                table[0, i].set_facecolor('#add8e6')

            pdf.savefig(fig)
            plt.clf()

            # Create detailed analysis for each cinema
            if "Roma" in list(table_data.keys()):
                sorted_cinema_names = [val[0] for val in table_data["Roma"]]
                sorted_cinema_names.extend([table_data[key][0][0] for key in table_data.keys() if key != "Roma"])
                if ("Roma" in list(df_cinemas.keys())):
                    df_cinemas["Roma"] = df_cinemas["Roma"][df_cinemas["Roma"]["Città"] == "Trento"]
            else:
                sorted_cinema_names = [table_data[key][0][0] for key in table_data.keys()]

            pbar = tg_tqdm(sorted_cinema_names, TOKEN, update.message.chat_id, total=len(sorted_cinema_names))

            for cinema in pbar:
                pbar.set_description(f"Sto processando il cinema {cinema}")

                table_data = []
                fig = plt.figure(dpi=100)
                movies = df_cinemas[cinema]["Titolo Film"]
                incassi_totali = 0
                presenze_totali = 0

                for i, movie in enumerate(movies):
                    target_row = df_cinemas[cinema].loc[df_merged['Titolo Film'] == movie]
                    incassi_film = target_row[columns_incasso].apply(pd.to_numeric).sum().sum()
                    presenze_film = target_row[columns_presenze].apply(pd.to_numeric).sum().sum()
                    costo_medio = round(incassi_film / presenze_film, 2)
                    table_data.append([twp.fill(movie, 40), round(incassi_film, 2), presenze_film, costo_medio])
                    incassi_totali += incassi_film
                    presenze_totali += presenze_film

                table_data.sort(key=lambda x: (-x[2]))
                table_data.append([r'\textbf{Totale}', round(incassi_totali, 2), presenze_totali, round(incassi_totali / presenze_totali, 2)])
                fig.suptitle(f"{cinema} ({df_cinemas[cinema]['Città'].iloc[0]}) - Incassi settimanali", fontsize=10)
                plt.axis('off')
                table = plt.table(cellText=table_data, loc='center', colLabels=column_detailed_labels, cellLoc="center")
                table.auto_set_font_size(False)
                table.set_fontsize(5)
                table.scale(1.2, 1.25)

                for i in range(len(column_detailed_labels)):
                    table[0, i].set_facecolor('#add8e6')

                pdf.savefig(fig)
                plt.clf()

            pdf.close()
            plt.close()
            await context.bot.send_document(update.message.chat_id, document=open(full_name, 'rb'))
    else:
        await update.message.reply_text("Non ho trovato file excel (.xlsx), fai l'upload dei file prima.")

    current_directory = os.getcwd()

    # List all files in the current directory
    files = os.listdir(current_directory)

    # Iterate through the files and remove each one, excluding those ending with ".py"
    for file in files:
        file_path = os.path.join(current_directory, file)
        if os.path.isfile(file_path) and not file.endswith(".py"):
            os.remove(file_path)


# Replace 'YOUR_BOT_TOKEN' with your actual bot token
with open('TOKEN', 'r') as file:
    token = file.readline()
    token.strip()

TOKEN = token

async def show_start_message(update: Update, _) -> None:
    help_message = '''Ciao! Inviami i file excel che desideri analizzare e io porterò avanti l'analisi.

Se vuoi sapere più info dammi il comando /aiuto :)
    '''
    await update.message.reply_text(help_message)

async def show_help_message(update: Update, _) -> None:
    help_message = '''Ciao!
Trascina in questa chat i file excel che vuoi analizzare, puoi farlo un file alla volta o tutti i file insieme.

Una volta che avrai inviato tutti i file, puoi iniziare l'operazione di analisi con il comando /analizza
La computazione dell'analisi prende dai 10 ai 30 secondi, una volta completata riceverai un pdf con i vari dati analizzati.

Una volta terminata, il bot cancella i dati ricevuti quindi se vorrai un'altra analisi, dovrai dargli dei nuovi file excel.

Se qualcosa non fosse chiaro scrivi a @NonenNone.
    '''
    await update.message.reply_text(help_message)

async def handle_files(update: Update, context: CallbackContext) -> None:
    file = await context.bot.get_file(update.message.document)

    n_xlsx_files = len(list(Path(".").glob('*.xlsx')))

    with open(f'{n_xlsx_files}.xlsx', 'wb') as buffer:
        await file.download_to_memory(buffer)

    await update.message.reply_text(f"Ho correttamente scaricato {n_xlsx_files + 1} file, grazie!")


async def send_joke(update: Update, _) -> None:
    api_url = 'https://official-joke-api.appspot.com/random_joke'
    try:
        response = requests.get(api_url)
        response.raise_for_status() 
        joke_data = response.json()
        await update.message.reply_text(f"{joke_data['setup']}\n\n{joke_data['punchline']}\n\nBadum tssssss....")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching joke: {e}")
        await update.message.reply_text("No joke for you.")


async def show_status(update: Update, _) -> None:
    n_xlsx_files = len(list(Path(".").glob('*.xlsx')))
    if n_xlsx_files == 0:
        help_message = "Il bot è operativo e non sta analizzando nessun file"
    else:
        help_message = f"Il bot è operativo ma è occupato analizzando {n_xlsx_files} file"

    await update.message.reply_text(help_message)


async def handle_error(update: Update, context: CallbackContext) -> None:

    logger.error("Exception while handling an update:", exc_info=context.error)

    # traceback.format_exception returns the usual python message about an exception, but as a
    # list of strings rather than a single string, so we have to join them together.
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)
    logger.error(tb_string, exc_info=context.error)

    current_directory = os.getcwd()
    files = os.listdir(current_directory)

    # Iterate through the files and remove each one, excluding those ending with ".py"
    for file in files:
        file_path = os.path.join(current_directory, file)
        if os.path.isfile(file_path) and not file.endswith(".py"):
            os.remove(file_path)

    # Finally, send the message
    await context.bot.send_message(
        chat_id=update.message.chat_id, text="Sono incappato in un errore, l'autore del bot è stato notificato.\nHo cancellato tutti i dati fino ad'ora quindi puoi ricominciare a mandarmi file.", parse_mode=ParseMode.HTML
    )


def main() -> None:
    application = Application.builder().token(TOKEN).build()

    # Define command handlers
    application.add_handler(CommandHandler("start", show_start_message))
    application.add_handler(CommandHandler("aiuto", show_help_message))
    application.add_handler(CommandHandler("analizza", process_xlsx))
    application.add_handler(CommandHandler("status", show_status))
    application.add_handler(CommandHandler("mario", send_joke))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_files))
    application.add_error_handler(handle_error)

    # Start the Bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
