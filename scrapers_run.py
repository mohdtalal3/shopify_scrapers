from scrapers.lululemon.lululemon import complete_workflow_lululemon
from scrapers.sportsdirect.sportsdirect import complete_workflow_sportsdirect
from scrapers.mytheresa.mytheresa import complete_workflow_mytheresa
from scrapers.hypefly.hypefly import complete_workflow_hypefly
from scrapers.youngla.youngla import complete_workflow_youngla
from scrapers.tory.tory import complete_workflow_tory
from scrapers.thedesignerboxuk.thedesignerboxuk import complete_workflow_thedesignerboxuk
from scrapers.shop437.shop437 import complete_workflow_shop437
from scrapers.polene_paris.polene_paris import complete_workflow_polene_paris
from scrapers.Notorious.notorious import complete_workflow_notorious
from scrapers.gymshark.gymshark import complete_workflow_gymshark
from scrapers.aloyoga.aloyoga import complete_workflow_aloyoga
from color_maps import run_color_mapping


def run_all_scrapers():
    scrapers = [
        ("Lululemon", complete_workflow_lululemon),
        ("Sports Direct", complete_workflow_sportsdirect),
        ("Mytheresa", complete_workflow_mytheresa),
        ("Hypefly", complete_workflow_hypefly),
        ("YoungLA", complete_workflow_youngla),
        ("Tory", complete_workflow_tory),
        ("The Designer Box UK", complete_workflow_thedesignerboxuk),
        ("Shop437", complete_workflow_shop437),
        ("Polene Paris", complete_workflow_polene_paris),
        ("Notorious", complete_workflow_notorious),
        ("Gymshark", complete_workflow_gymshark),
        ("Alo Yoga", complete_workflow_aloyoga)
    ]
    
    for scraper_name, scraper_function in scrapers:
        try:
            print(f"\n🔄 Starting {scraper_name} scraper...")
            scraper_function()
            print(f"✅ {scraper_name} scraper completed successfully")
        except Exception as e:
            print(f"❌ {scraper_name} scraper failed with error: {str(e)}")
            print(f"   Continuing with next scraper...")
    
    # Run color mapping at the end
    try:
        print(f"\n🔄 Starting color mapping...")
        run_color_mapping()
        print(f"✅ Color mapping completed successfully")
    except Exception as e:
        print(f"❌ Color mapping failed with error: {str(e)}")
        print(f"   Continuing...")

if __name__ == "__main__":
    run_all_scrapers()