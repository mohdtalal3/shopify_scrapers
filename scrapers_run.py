from scrapers.lululemon.lululemon import complete_workflow_lululemon
from scrapers.sportsdirect.sportsdirect import complete_workflow_sportsdirect
from scrapers.mytheresa.mytheresa import complete_workflow_mytheresa
from scrapers.hypefly.hypefly import complete_workflow_hypefly
from scrapers.youngla.youngla import complete_workflow_youngla
from scrapers.tory.tory import complete_workflow_tory
from scrapers.thedesignerboxuk.thedesignerboxuk import complete_workflow_thedesignerboxuk
from scrapers.shop437.shop437 import complete_workflow_437
from scrapers.polene_paris.polene_paris import complete_workflow_polene_paris
from scrapers.Notorious.notorious import complete_workflow_notorious
from scrapers.gymshark.gymshark import complete_workflow_gymshark
from scrapers.aloyoga.aloyoga import complete_workflow_aloyoga
from scrapers.araks.araks import complete_workflow_araks
from scrapers.balardi.balardi import complete_workflow_balardi
from scrapers.bandi.bandi import complete_workflow_bandit_running as complete_workflow_bandi
from scrapers.blssdfashion.blssdfashion import complete_workflow_blssdfashion
from scrapers.boohoo.boohoo import complete_workflow_boohoo
from scrapers.coach.coach import complete_workflow_coachoutlet
from scrapers.cocodemer.cocodemer import complete_workflow_fruitybooty as complete_workflow_cocodemer
from scrapers.cosabella.cosabella import complete_workflow_cosabella
from scrapers.cruise_fashion.cruise_fashion import complete_workflow_cruise_fashion
from scrapers.eberjey.eberjey import complete_workflow_eberjey
from scrapers.fruitybooty.fruitybooty import complete_workflow_fruitybooty
from scrapers.gemopticians.gemopticians import complete_workflow_gemopticians
from scrapers.hustle_culture.hustle_culture import complete_workflow_hustle_culture
from scrapers.karl.karl import complete_workflow_uk_polene as complete_workflow_karl
from scrapers.kate.kate import complete_workflow_kate
from scrapers.katspade_outlet.kateoutlet import complete_workflow_kate_outlet
from scrapers.laperla.laperla import complete_workflow_laperla
from scrapers.livetheprocess.livetheprocess import complete_workflow_livetheprocess
from scrapers.lounge.lounge import complete_workflow_lounge
from scrapers.marcjacobs.marcjacobs import complete_workflow_marc_jacobs
from scrapers.meshki.meshki import complete_workflow_meshki
from scrapers.nakedwolfe.nakedwolf import complete_workflow_nakedwolf
from scrapers.oh_polly_uk.oh_polly_uk import complete_workflow_oh_polly_uk
from scrapers.organicbasics.organicbasics import complete_workflow_organicbasics
from scrapers.prettylittlething.prettylittlething import complete_workflow_pretty_little_things
from scrapers.ratandboa.ratandboa import complete_workflow_ratandboa
from scrapers.rhodeskin.rhodeskin import complete_workflow_polene_paris as complete_workflow_rhodeskin
from scrapers.riverisland.riverisland import complete_workflow_river
from scrapers.shop_whoop.shop_whoop import complete_workflow_shop_whoop
from scrapers.skims.skims import complete_workflow_skims
from scrapers.sportyandrich.sportyandrich import complete_workflow_sportyandrich
from scrapers.stanley.stanley import complete_workflow_stanley
from scrapers.superdown.superdown import complete_workflow_superdown
from scrapers.thereformation.thereformation import complete_workflow_thereformation
from scrapers.uk_polene.uk_polene import complete_workflow_uk_polene
from scrapers.underarmour.underarmour import complete_workflow_underarmour
from scrapers.vaara.vaara import complete_workflow_vaara
from scrapers.victoria.victoria import complete_workflow_victoria
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
        ("Shop437", complete_workflow_437),
        ("Polene Paris", complete_workflow_polene_paris),
        ("Notorious", complete_workflow_notorious),
        ("Gymshark", complete_workflow_gymshark),
        ("Alo Yoga", complete_workflow_aloyoga),
        ("Araks", complete_workflow_araks),
        ("Balardi", complete_workflow_balardi),
        ("Bandi", complete_workflow_bandi),
        ("BLSSD Fashion", complete_workflow_blssdfashion),
        ("Boohoo", complete_workflow_boohoo),
        ("Coach Outlet", complete_workflow_coachoutlet),
        ("Cocodemer", complete_workflow_cocodemer),
        ("Cosabella", complete_workflow_cosabella),
        ("Cruise Fashion", complete_workflow_cruise_fashion),
        ("Eberjey", complete_workflow_eberjey),
        ("Fruity Booty", complete_workflow_fruitybooty),
        ("Gem Opticians", complete_workflow_gemopticians),
        ("Hustle Culture", complete_workflow_hustle_culture),
        ("Karl", complete_workflow_karl),
        ("Kate", complete_workflow_kate),
        ("Kate Spade Outlet", complete_workflow_kate_outlet),
        ("La Perla", complete_workflow_laperla),
        ("Live The Process", complete_workflow_livetheprocess),
        ("Lounge", complete_workflow_lounge),
        ("Marc Jacobs", complete_workflow_marc_jacobs),
        ("Meshki", complete_workflow_meshki),
        ("Naked Wolfe", complete_workflow_nakedwolf),
        ("Oh Polly UK", complete_workflow_oh_polly_uk),
        ("Organic Basics", complete_workflow_organicbasics),
        ("Pretty Little Thing", complete_workflow_pretty_little_things),
        ("Rat and Boa", complete_workflow_ratandboa),
        ("Rhode Skin", complete_workflow_rhodeskin),
        ("River Island", complete_workflow_river),
        ("Shop Whoop", complete_workflow_shop_whoop),
        ("Skims", complete_workflow_skims),
        ("Sporty and Rich", complete_workflow_sportyandrich),
        ("Stanley", complete_workflow_stanley),
        ("Superdown", complete_workflow_superdown),
        ("The Reformation", complete_workflow_thereformation),
        ("UK Polene", complete_workflow_uk_polene),
        ("Under Armour", complete_workflow_underarmour),
        ("Vaara", complete_workflow_vaara),
        ("Victoria", complete_workflow_victoria)
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