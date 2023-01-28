import asyncio
import json
from typing import Dict, List, Tuple, TypedDict
from urllib.parse import urlencode, urljoin

from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient


def parse_search(result: ScrapeApiResponse) -> Tuple[List[Dict], Dict]:
    """
    Parses yelp search results for business results
    Returns list of businesses and search metadata
    """
    search_results = json.loads(result.content)
    results = search_results["searchPageProps"]["mainContentComponentsListProps"]
    businesses = [r for r in results if r.get("searchResultBusiness") and not r.get("adLoggingInfo")]
    search_meta = next(r for r in results if r.get("type") == "pagination")["props"]
    return businesses, search_meta


class Company(TypedDict):
    name: str
    website: str
    phone: str
    address: str
    logo: str
    open_hours: dict[str, str]
    claim_status: str


def parse_company(result: ScrapeApiResponse):
    xpath = lambda xp: result.selector.xpath(xp).get(default="").strip()
    open_hours = {}
    for day in result.selector.xpath('//th/p[contains(@class,"day-of-the-week")]'):
        name = day.xpath("text()").get().strip()
        value = day.xpath("../following-sibling::td//p/text()").get().strip()
        open_hours[name.lower()] = value

    claim_status = (
        "".join(result.selector.xpath('//span[contains(@class,"claim-text")]/text()').getall()).strip().lower()
    )
    return dict(
        name=xpath("//h1/text()"),
        website=xpath('//p[contains(text(),"Business website")]/following-sibling::p/a/text()'),
        phone=xpath('//p[contains(text(),"Phone number")]/following-sibling::p/text()'),
        address=xpath('//a[contains(text(),"Get Directions")]/../following-sibling::p/text()'),
        logo=xpath('//img[contains(@class,"businessLogo")]/@src'),
        open_hours=open_hours,
        claim_status=claim_status,
    )


def create_search_url(keyword: str, location: str, offset=0):
    """scrape single page of yelp search"""
    return "https://www.yelp.com/search/snippet?" + urlencode(
        {
            "find_desc": keyword,
            "find_loc": location,
            "start": offset,
            "parent_request": "",
            "ns": 1,
            "request_origin": "user",
        }
    )


async def search_yelp(keyword: str, location: str, session: ScrapflyClient):
    """scrape all pages of yelp search for business preview data"""
    first_page = await session.async_scrape(ScrapeConfig(create_search_url(keyword, location)))
    businesses, search_meta = parse_search(first_page)

    other_urls = [create_search_url(keyword, location, page) for page in range(10, search_meta["totalResults"], 10)]
    async for result in session.concurrent_scrape([ScrapeConfig(url) for url in other_urls]):
        businesses.extend(parse_search(result)[0])
    return businesses


async def _scrape_companies_by_url(urls: List[str], session: ScrapflyClient) -> List[Dict]:
    """Scrape yelp company details from given yelp company urls"""
    results = []
    async for result in session.concurrent_scrape([ScrapeConfig(url) for url in urls]):
        results.append(parse_company(result))
    return results


async def scrape_companies_by_search(keyword: str, location: str, session: ScrapflyClient):
    """Scrape yelp company detail from given search details"""
    found_company_previews = await search_yelp(keyword, location, session=session)
    company_urls = [
        urljoin(
            "https://www.yelp.com",
            company_preview["searchResultBusiness"]["businessUrl"],
        )
        for company_preview in found_company_previews
    ]
    return await _scrape_companies_by_url(company_urls, session=session)

class Review(TypedDict):
    id: str
    userId: str
    business: dict
    user: dict
    comment: dict
    rating: int
    ...

async def scrape_reviews(business_url: str, session: ScrapflyClient) -> List[Review]:
    result_business = await session.async_scrape(ScrapeConfig(business_url))
    business_id = result_business.selector.css('meta[name="yelp-biz-id"]::attr(content)').get()
    first_page = await session.async_scrape(
        ScrapeConfig(f"https://www.yelp.com/biz/{business_id}/review_feed?rl=en&q=&sort_by=relevance_desc&start=0")
    )
    first_page_data = json.loads(first_page.content)
    reviews = first_page_data["reviews"]
    total_reviews = first_page_data["pagination"]["totalResults"]
    print(f"scraping {total_reviews} of business {business_id}")
    to_scrape = [
        ScrapeConfig(
            f"https://www.yelp.com/biz/{business_id}/review_feed?rl=en&q=&sort_by=relevance_desc&start={offset}"
        )
        for offset in range(10, total_reviews + 10, 10)
    ]
    async for result in session.concurrent_scrape(to_scrape):
        data = json.loads(result.content)
        reviews.extend(data["reviews"])
    return reviews


async def run():
    scrapfly = ScrapflyClient(key="scp-live-18d16f85e76f4607b72bccbec179ce0d", max_concurrency=2)
    with scrapfly as session:
        results = await scrape_companies_by_search("Bar", "Kinnelon, New Jersey", session=session)
        total = json.dumps(results)
        print(total)
        return total


if __name__ == "__main__":
    asyncio.run(run())