import httpx
import pandas as pd
from bs4 import BeautifulSoup


def createPersonData(person, browser_headers):
    # Get name
    name = person.find("a").text

    # Get personal page
    personal_url = "https://en.wikipedia.org" + person.find("a", href=True)["href"]
    personal_page_response = httpx.get(personal_url, headers=browser_headers)
    personal_page = BeautifulSoup(personal_page_response.text, features="html.parser")

    # Get number of awards
    page_text = personal_page.select(
        "div.mw-page-container-inner div.mw-content-container main#content div#bodyContent div#mw-content-text div.mw-content-ltr.mw-parser-output *"
    )
    awardsFlag = False
    num_awards = 0
    for element in page_text:
        if "Awards" in element.text and (element.name == "h2" or element.name == "h3"):
            awardsFlag = True
        elif awardsFlag is True and element.name == "li":
            num_awards += 1
        elif awardsFlag is True and (element.name == "h2" or element.name == "h3"):
            awardsFlag = False
    if num_awards == 0:
        num_awards = "NaN"

    # Get institutions
    institutions = []
    potential_institutions_elements = personal_page.select(
        "div#mw-content-text div.mw-content-ltr.mw-parser-output table.infobox.biography.vcard tbody tr"
    )
    for potential_institutions_element in potential_institutions_elements:
        potential_institutions_element_th = potential_institutions_element.select("th")
        if (
            len(potential_institutions_element_th) > 0
            and potential_institutions_element_th[0].text == "Institutions"
        ):
            institutions_elements = potential_institutions_element.select("a")
            for institution_element in institutions_elements:
                if "[" not in institution_element.text:
                    institutions.append(institution_element.text)

    return name, num_awards, institutions


def main():
    print("Started scraping list of computer scientits")
    url = "https://en.wikipedia.org/wiki/List_of_computer_scientists"
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/119.0"
    }

    response = httpx.get(url, headers=browser_headers)
    html = BeautifulSoup(response.text, features="html.parser")

    bullets = html.select("div.mw-content-ltr.mw-parser-output ul li")

    people = []
    namesFlag = False
    for bullet in bullets:
        if (
            bullet.text
            == "Atta ur Rehman Khan – Mobile Cloud Computing, Cybersecurity, IoT"
        ):
            namesFlag = True
        elif bullet.text == "Konrad Zuse – German pioneer of hardware and software":
            namesFlag = False
        if namesFlag:
            people.append(bullet)

    persons_data = []

    i = 1
    for person in people:
        print(f"\rScraping data for person {i}/{len(people)}", end="", flush=True)
        surname, num_awards, institutions = createPersonData(person, browser_headers)
        for institution in institutions:
            persons_data.append([surname, num_awards, institution])
        i += 1
    print("\nFinished")

    df = pd.DataFrame(persons_data, columns=["Name", "Awards", "Institution"])
    df.to_csv("../dataset/list_of_computer_scientists.csv", index=False)


main()
