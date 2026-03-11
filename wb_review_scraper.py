"""
Wildberries Review Scraper
Парсит все отзывы с изображениями для списка артикулов товаров.

Использование:
    python wb_review_scraper.py --urls "https://www.wildberries.ru/catalog/237519806/feedbacks"
    python wb_review_scraper.py --file urls.txt
    python wb_review_scraper.py --articles 237519806 123456789
"""

import re
import json
import time
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Wildberries public API endpoints ────────────────────────────────────────
CARD_API = "https://card.wb.ru/cards/v1/detail"
FEEDBACKS_API = "https://feedbacks2.wb.ru/feedbacks/v1/all"
PHOTO_BASE = "https://feedbacksphotos.wb.ru"   # photos are served from here

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://www.wildberries.ru",
    "Referer": "https://www.wildberries.ru/",
}
PAGE_SIZE = 30          # WB returns at most 30 reviews per request
REQUEST_DELAY = 0.5     # seconds between requests (be polite)
MAX_RETRIES = 3


# ─── Data classes ─────────────────────────────────────────────────────────────
@dataclass
class Photo:
    url: str
    local_path: str = ""


@dataclass
class Review:
    review_id: str
    nm_id: int
    imt_id: int
    product_name: str
    author: str
    rating: int
    date: str
    text: str
    pros: str
    cons: str
    photos: list[Photo] = field(default_factory=list)


# ─── Helpers ──────────────────────────────────────────────────────────────────
def extract_article(url: str) -> Optional[int]:
    """Извлекает артикул (nmId) из URL вида /catalog/{nmId}/feedbacks"""
    m = re.search(r"/catalog/(\d+)/", url)
    return int(m.group(1)) if m else None


def get_with_retry(session: requests.Session, url: str, params: dict) -> Optional[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, url, exc)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    return None


def get_imt_id(session: requests.Session, nm_id: int) -> Optional[int]:
    """Получает imtId (id карточки товара) по артикулу nmId."""
    data = get_with_retry(
        session,
        CARD_API,
        {"appType": 1, "curr": "rub", "dest": -1257786, "nm": nm_id},
    )
    try:
        products = data["data"]["products"]
        if not products:
            log.error("Товар с артикулом %d не найден", nm_id)
            return None
        imt_id = products[0]["root"]
        name = products[0].get("name", "")
        log.info("Артикул %d → imtId=%d  «%s»", nm_id, imt_id, name)
        return imt_id, name
    except (TypeError, KeyError, IndexError) as exc:
        log.error("Не удалось разобрать ответ для %d: %s", nm_id, exc)
        return None


def build_photo_url(photo: dict) -> str:
    """Формирует полный URL фото из объекта в ответе WB."""
    # WB отдаёт относительные пути, напр. /v1/photos/nm/...
    full_url = photo.get("fullSizeUri", "") or photo.get("previewUri", "")
    if full_url.startswith("http"):
        return full_url
    return PHOTO_BASE + full_url


def fetch_all_reviews(session: requests.Session, imt_id: int, nm_id: int, product_name: str) -> list[Review]:
    """Постранично забирает все отзывы для imtId."""
    reviews: list[Review] = []
    skip = 0

    while True:
        data = get_with_retry(
            session,
            FEEDBACKS_API,
            {
                "imtId": imt_id,
                "skip": skip,
                "take": PAGE_SIZE,
                "order": "dateDesc",
                "hasPhoto": 0,     # 0 = все отзывы; 1 = только с фото
            },
        )
        if not data:
            break

        feedbacks = data.get("feedbacks") or []
        if not feedbacks:
            break

        for fb in feedbacks:
            photos_raw = fb.get("photos") or []
            photos = [Photo(url=build_photo_url(p)) for p in photos_raw if build_photo_url(p)]

            reviews.append(
                Review(
                    review_id=fb.get("id", ""),
                    nm_id=nm_id,
                    imt_id=imt_id,
                    product_name=product_name,
                    author=fb.get("wbUserDetails", {}).get("name", "Аноним"),
                    rating=fb.get("productValuation", 0),
                    date=fb.get("createdDate", ""),
                    text=fb.get("text", ""),
                    pros=fb.get("pros", ""),
                    cons=fb.get("cons", ""),
                    photos=photos,
                )
            )

        log.info("  Получено отзывов: %d (всего загружено: %d)", len(feedbacks), len(reviews))

        # Если вернулось меньше страницы — больше нет
        if len(feedbacks) < PAGE_SIZE:
            break

        skip += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    return reviews


def download_photos(
    session: requests.Session,
    reviews: list[Review],
    out_dir: Path,
) -> int:
    """Скачивает фото всех отзывов. Возвращает число скачанных файлов."""
    total = 0
    for review in reviews:
        if not review.photos:
            continue
        review_dir = out_dir / str(review.nm_id) / review.review_id
        review_dir.mkdir(parents=True, exist_ok=True)
        for idx, photo in enumerate(review.photos):
            ext = photo.url.split("?")[0].rsplit(".", 1)[-1] or "jpg"
            filename = review_dir / f"{idx + 1}.{ext}"
            if filename.exists():
                photo.local_path = str(filename)
                total += 1
                continue
            try:
                resp = session.get(photo.url, timeout=20, stream=True)
                resp.raise_for_status()
                filename.write_bytes(resp.content)
                photo.local_path = str(filename)
                total += 1
                log.debug("    Сохранено: %s", filename)
            except Exception as exc:
                log.warning("Не удалось скачать %s: %s", photo.url, exc)
    return total


def save_results(reviews: list[Review], out_dir: Path):
    """Сохраняет результаты в JSON файлы по артикулам."""
    # Группировка по nm_id
    by_article: dict[int, list[Review]] = {}
    for r in reviews:
        by_article.setdefault(r.nm_id, []).append(r)

    for nm_id, item_reviews in by_article.items():
        # Отзывы с фото
        with_photos = [r for r in item_reviews if r.photos]
        out_file = out_dir / f"{nm_id}_reviews.json"
        data = [asdict(r) for r in item_reviews]
        out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        log.info(
            "Артикул %d: всего %d отзывов, из них с фото %d → %s",
            nm_id,
            len(item_reviews),
            len(with_photos),
            out_file,
        )


# ─── Main ─────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Wildberries review + photo scraper")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--urls", nargs="+", metavar="URL",
                     help="Список URL вида https://www.wildberries.ru/catalog/NM/feedbacks")
    src.add_argument("--file", metavar="FILE",
                     help="Файл со списком URL (по одному на строку)")
    src.add_argument("--articles", nargs="+", type=int, metavar="NM",
                     help="Список артикулов напрямую")
    p.add_argument("--output", default="wb_output", metavar="DIR",
                   help="Папка для результатов (по умолчанию: wb_output)")
    p.add_argument("--no-photos", action="store_true",
                   help="Не скачивать фото, только сохранить JSON")
    p.add_argument("--only-with-photos", action="store_true",
                   help="Выгружать только отзывы с фотографиями")
    return p.parse_args()


def main():
    args = parse_args()
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)

    # Собираем артикулы
    nm_ids: list[int] = []
    if args.articles:
        nm_ids = args.articles
    elif args.urls:
        for url in args.urls:
            nm = extract_article(url)
            if nm:
                nm_ids.append(nm)
            else:
                log.warning("Не удалось извлечь артикул из URL: %s", url)
    elif args.file:
        with open(args.file) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                nm = extract_article(line)
                if nm:
                    nm_ids.append(nm)
                else:
                    log.warning("Не удалось извлечь артикул из строки: %s", line)

    if not nm_ids:
        log.error("Не найдено ни одного артикула.")
        return

    log.info("Артикулов для обработки: %d", len(nm_ids))

    session = requests.Session()
    session.headers.update(HEADERS)

    all_reviews: list[Review] = []

    for nm_id in nm_ids:
        log.info("── Обработка артикула %d ──────────────────────", nm_id)

        result = get_imt_id(session, nm_id)
        if result is None:
            continue
        imt_id, product_name = result

        reviews = fetch_all_reviews(session, imt_id, nm_id, product_name)

        if args.only_with_photos:
            reviews = [r for r in reviews if r.photos]

        log.info("  Итого отзывов: %d", len(reviews))
        all_reviews.extend(reviews)
        time.sleep(REQUEST_DELAY)

    if not all_reviews:
        log.info("Отзывов не найдено.")
        return

    # Скачиваем фото
    if not args.no_photos:
        photos_dir = out_dir / "photos"
        photos_dir.mkdir(exist_ok=True)
        reviews_with_photos = [r for r in all_reviews if r.photos]
        total_photos = sum(len(r.photos) for r in reviews_with_photos)
        log.info(
            "Скачиваем фото: %d отзывов с изображениями, %d фото",
            len(reviews_with_photos),
            total_photos,
        )
        downloaded = download_photos(session, all_reviews, photos_dir)
        log.info("Скачано фото: %d", downloaded)

    # Сохраняем JSON
    save_results(all_reviews, out_dir)
    log.info("Готово. Результаты сохранены в папку: %s", out_dir)


if __name__ == "__main__":
    main()
