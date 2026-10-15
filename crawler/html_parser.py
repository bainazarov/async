import logging
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class HTMLParser:

    @staticmethod
    def extract_metadata(soup: BeautifulSoup) -> dict:
        title = str(soup.title.string).strip() if soup.title and soup.title.string else ""

        def _meta(name: str) -> str:
            tag = soup.find("meta", attrs={"name": name}) or \
                  soup.find("meta", attrs={"property": f"og:{name}"})
            return str(tag.get("content", "")).strip() if tag else ""

        return {
            "title": title,
            "description": _meta("description"),
            "keywords": _meta("keywords"),
        }

    @staticmethod
    def extract_links(soup: BeautifulSoup, base_url: str) -> list[str]:
        links = []
        for tag in soup.find_all("a", href=True):
            href = str(tag["href"]).strip()
            if not href or href.startswith(("#", "mailto:", "javascript:")):
                continue
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
            if parsed.scheme in ("http", "https"):
                links.append(absolute)
        return links

    @staticmethod
    def extract_text(soup: BeautifulSoup, selector: str | None = None) -> str:
        target = soup.select_one(selector) if selector else soup
        if not target:
            return ""
        for tag in target(["script", "style", "noscript"]):
            tag.decompose()
        return " ".join(target.get_text(separator=" ").split())

    @staticmethod
    def extract_images(soup: BeautifulSoup) -> list[dict]:
        return [
            {"src": img.get("src", ""), "alt": img.get("alt", "")}
            for img in soup.find_all("img")
            if img.get("src")
        ]

    @staticmethod
    def extract_headings(soup: BeautifulSoup) -> list[dict]:
        headings = []
        for tag in soup.find_all(["h1", "h2", "h3"]):
            text = tag.get_text(separator=" ").strip()
            if text:
                headings.append({"level": tag.name, "text": text})
        return headings

    @staticmethod
    def extract_tables(soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = []
        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(separator=" ").strip()
                         for td in tr.find_all(["td", "th"])]
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables

    @staticmethod
    def extract_lists(soup: BeautifulSoup) -> list[list[str]]:
        result = []
        for lst in soup.find_all(["ul", "ol"]):
            items = [li.get_text(separator=" ").strip()
                     for li in lst.find_all("li")]
            if items:
                result.append(items)
        return result

    async def parse_html(self, html: str, url: str) -> dict:
        if not html:
            logger.warning("Пустой HTML: %s", url)
            return {"url": url, "error": "empty html"}

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as e:
            logger.warning("Ошибка парсинга %s: %s", url, e)
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception as e2:
                logger.error("Не удалось распарсить %s: %s", url, e2)
                return {"url": url, "error": str(e2)}

        metadata = self.extract_metadata(soup)
        links = self.extract_links(soup, url)
        text = self.extract_text(soup)
        images = self.extract_images(soup)
        headings = self.extract_headings(soup)
        tables = self.extract_tables(soup)
        lists = self.extract_lists(soup)

        logger.info("Распарсено: %s | ссылок=%d, изображений=%d, символов=%d",
                    url, len(links), len(images), len(text))

        return {
            "url": url,
            "title": metadata["title"],
            "metadata": metadata,
            "text": text,
            "text_length": len(text),
            "links": links,
            "links_count": len(links),
            "images": images,
            "images_count": len(images),
            "headings": headings,
            "tables": tables,
            "lists": lists,
        }
