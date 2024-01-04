from collections.abc import Generator
from contextlib import contextmanager
from typing import Iterable
from pathlib import Path

from retry import retry

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

import pandas as pd

from tqdm.auto import tqdm

URL = "https://euroauto.ru/part/new/"

class ParserDriver:

    def __init__(self, url: str) -> None:
        self.driver = webdriver.Firefox()
        self.driver.get(url)

    @staticmethod
    @contextmanager
    def create_driver(url: str) -> Generator["ParserDriver", None, None]:
        parser_driver = ParserDriver(url)
        yield parser_driver
        parser_driver.stop()

    @retry((TimeoutException, StaleElementReferenceException), tries=2)
    def generate_list_of_values(self, articule: str, timeout_limit: float = 7) -> list[str]:
        input_form = WebDriverWait(self.driver, timeout_limit).until(
            EC.presence_of_element_located((By.CLASS_NAME, "search-form__input"))
        )
        input_form.clear()
        input_form.send_keys(articule, Keys.ENTER)
        current_url = self.driver.current_url
        while True:
            element = WebDriverWait(self.driver, timeout_limit).until(
                EC.all_of(
                    EC.any_of(
                        EC.presence_of_element_located((By.ID, "product-new-block")),
                        EC.presence_of_element_located((By.CLASS_NAME, "blue-button"))
                    ),
                    EC.url_changes(current_url)
                )
            )[0]

            if element.get_attribute("class") == "blue-button":
                current_url = self.driver.current_url
                element.click()
            else:
                break
        bs_main_block = BeautifulSoup(self.driver.page_source, "html.parser")
        name_detail_block = bs_main_block.find("div", class_="part-h1")
        applied_autos_list_block = bs_main_block.find(class_="modal-part-compatible-list")
        note = None
        for table_row in bs_main_block.find("table", class_="part-parameters-table").find_all(
            "tr", class_="part-parameters-row-block"
        ):
            if table_row.find("td", class_="part-parameters-row-block-data").text == "Примечание":
                note = table_row.find("td", class_="part-parameters-row-block-value").text
                break

        return (
            articule,
            name_detail_block.h1.text,
            note,
            tuple(map(lambda x: x.text, applied_autos_list_block.find_all("li"))) \
                if applied_autos_list_block is not None else None
        )

    def iterate_over_articules(self, articules: Iterable[str]) -> tuple[pd.DataFrame, list[str]]:
        info = []
        errors = []
        for app in tqdm(articules, leave=True):
            try:
                info.append(self.generate_list_of_values(app))
            except TimeoutException:
                info.append((app, None, None, None))
                errors.append(app)
            except AttributeError:
                print("some info absents")
                info.append((app, None, None, None))
                errors.append(app)
        return info, errors

    def stop(self) -> None:
        self.driver.close()


if __name__ == "__main__":
    articules = pd.read_excel(Path("data/articules2.xlsx"))["Number"].values
    save_directory = Path("./save_info")
    save_directory.mkdir(parents=True, exist_ok=True)

    with ParserDriver.create_driver(URL) as driver:
        for i in tqdm(range(0, len(articules), 100), total=len(articules) // 100 + 1):
            info, errors = driver.iterate_over_articules(articules[i:i + 100])
            for j in filter(
                lambda k: info[k] is not None and info[k][3] is not None,
                range(len(info))
            ):
                info[j] = info[j][:3] + (';'.join(info[j][3]),)

            save_directory_iter = save_directory.joinpath(f"data_{i}_{i + 100}")
            save_directory_iter.mkdir(exist_ok=True)
            pd.DataFrame(
                data=info, columns=("Артикул", "Название", "Примечание", "Авто")
            ).to_csv(save_directory_iter.joinpath("info.csv"))

            with open(
                save_directory_iter.joinpath("errors.txt"), 'w', encoding="utf-8"
            ) as error_file:
                error_file.writelines(error + '\n' for error in errors)
