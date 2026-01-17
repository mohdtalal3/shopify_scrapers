from scrapers.kate.kate import complete_workflow_kate
from scrapers.katspade_outlet.kateoutlet import complete_workflow_kate_outlet
from scrapers.coach.coach import complete_workflow_coachoutlet
try:
    from color_maps import run_color_mapping
except ImportError:
    print("Warning: color_maps module not found. Color mapping will be skipped")
    def run_color_mapping():
        print("Color mapping skipped - module not available")
        pass


def display_menu():
    """Display the scraper menu"""
    print("\n" + "="*50)
    print("          SCRAPER MENU")
    print("="*50)
    print("1. Kate Spade")
    print("2. Kate Spade Outlet")
    print("3. Coach Outlet")
    print("4. Run All")
    print("0. Exit")
    print("="*50)


def run_kate():
    """Run Kate Spade scraper"""
    print("\n🔄 Running Kate Spade scraper...")
    complete_workflow_kate()
    print("✓ Kate Spade scraper completed\n")


def run_kate_outlet():
    """Run Kate Spade Outlet scraper"""
    print("\n🔄 Running Kate Spade Outlet scraper...")
    complete_workflow_kate_outlet()
    print("✓ Kate Spade Outlet scraper completed\n")


def run_coach_outlet():
    """Run Coach Outlet scraper"""
    print("\n🔄 Running Coach Outlet scraper...")
    complete_workflow_coachoutlet()
    print("✓ Coach Outlet scraper completed\n")


def run_all_scrapers():
    """Run all scrapers sequentially"""
    print("\n🔄 Running all scrapers...\n")
    run_kate()
    run_kate_outlet()
    run_coach_outlet()



def main():
    """Main function to handle user input"""
    while True:
        display_menu()
        choice = input("Enter your choice (0-4): ").strip()
        
        if choice == "1":
            run_kate()
        elif choice == "2":
            run_kate_outlet()
        elif choice == "3":
            run_coach_outlet()
        elif choice == "4":
            run_all_scrapers()
        elif choice == "0":
            print("\n👋 Exiting... Goodbye!\n")
            break
        else:
            print("\n❌ Invalid choice. Please enter a number between 0 and 4.\n")

    print("\n🔄 Running color mapping...")
    run_color_mapping()
    print("✓ All scrapers completed!\n")

if __name__ == "__main__":
    main()