import requests
import time

from src.models.bet import Bet 
from typing import List, Optional 


# ----------------------------
# USER LIST (username, id)
# ----------------------------
USERS = [
    ("Shahbazahmad", "SkQHBTMm0kPgvFLfDzppbxqjbHk1"),
    ("13d6", "7TZzlxaneOTCS1KZQjb5dVRPAD02"),
    ("AnichurRahman", "H4DJhtu5ZCY8F8bvjimS4An8LB12"),
    ("Andyed45", "vBs0gWLAywN9KHdvd1ahOABmblA3"),
    ("MdJoni", "Zk1PiaZdcHQCO3ZgjlpRxQZI80L2"),
    ("compliancesolutionscanada", "zcHuwV7eFObdk4YOga7ybKt6vEc2"),
    ("mksportred", "eGuMNlnGGnUFqtTRWjrWg96IKb23"),
    ("Farazarain", "aPVGIc9hAZSvPjxeMhSWHBvRImv1"),
    ("Mkmahi", "z8iQcYcvnYUUwW6DVvRM57dXfpG2"),
    ("NayanmoniBora", "ilTDhTTJtad54TSFeeOLakcBrmn1"),
    ("NayanmoniBori", "M9tvup6XX9NTmzPttocmrvu1mvt1"),
    ("Suvo", "hqI7sXJp9NWQlacNS7cTxb4tKZL2"),
    ("VickySingh", "DBPIYsFvWKb0SzkdaaySkTykcYn1"),
    ("4e1c", "Falpdar1MSTnCuco7FZDrd6k8Af1"),
    ("MalikRazaq", "Yaef6BY739ajg852HkfBjeb3fZR2"),
    ("PahkiJonaide", "p2xM2954PDc9JMQQtuKWVaOxV4t1"),
    ("Abdulmanan", "sNekNhMNUteojp86zznmsOsTkAX2"),
    ("OmarFaruk", "sZStaIwm3Ng1MH6De5djPL5GODJ2"),
    ("RahimKing", "KOlyZ9F8snhTCdtjSfjaQWeoQCn1"),
    ("RandyClinton", "Om534WzaLxWIGlyH5ff4o3xy4zJ3"),
    ("GfdhudDghgt", "zsw1cqaVT6UMTWTq3dYCLbOK9Rw2"),
    ("MdMoktar", "5cRgVZrxjMOkKCYH40EFlKulPor2"),
    ("DeolindaCamueleLidy", "R5ZwS12cnJUy54GaTrFO1jXOrUI3"),
    ("VijayKate", "zTAINKBAFlXwludXrn1HNFU1Q5i2"),
    ("InstasWppp", "lmrzp37nElfnCXFWjBa1Rpz4RSf1"),
    ("Demrer", "8Fuuec92zZa7eJqWbJXOw68wD0v1"),
    ("JasonSparer", "i0FQibMe7DQFE9oPydgJPIrn4za2"),
    ("eeke1ddf", "peD2qEVIT3PmU6MdBlhBAFbs5Lc2"),
    ("IClq6P", "rgRM0L6rPQMmktiLlvXuX1YeH6B3"),
    ("KoenvandenBerg39dc", "CqPROplDHYWGu2kvrOqxw5PYjOq1"),
    ("MatthewSalemme", "qn0nBknfA1UGmzOEvMU7qUX44Tg2"),
    ("JuliaBurnham", "DqVWJoi2wyTn7VY0uoKMXBuh3KG2"),
    ("Eugenec289", "YFRqI4EsvqeEo6xztpzucCIVTDq1"),
    ("GideonRosenblatt", "dAl3DkTp2jZAv7XxXqQtty8v0JB3"),
    ("MattAntonczyk", "JthAgHagtJNtUY212RgoMp7MM5j2"),
    ("EddyGao", "ctlJw5KpkTgqjwGNDnGaiLeFrZG3"),
    ("IrmisAlija", "5tgv25z483ZyTPyFTsynIGMt7t03"),
    ("SalvianoMcdonald", "DQ2tTPNyYjbJjbeUA6ZOiMun7zo2"),
    ("Senseimonkey", "hJ3qek0Ar1ZmZ7akNr1DFnOTwZf1"),
    ("lq66", "vLDbnLUeXOgWj7ZZPI0bhzo5gGg1"),
    ("hb88hz8z", "qR60bm9RxHNXHigypVqbDuRwzjJ2"),
    ("manclubnmfn", "zdEmWey0CzcbbmA9WG5jK69Ityi2"),
    ("CarlAagaardMadsen", "fhLdpU7TbaS1LO42sUJWchBwxxu2"),
    ("Osias", "cjLxC2wmErexXMjNp6Hh8ZzqKzW2"),
    ("jiangnanooo", "q0z51unKltceSlvSnUJVtRq9yVP2"),
    ("Robertsbwq", "RZwfpexmMsdfVGRqKVWt78Dza9s2"),
    ("phadindrauprety", "mxUJLmKIdVWpGcEiq8SKP5yD8qq1"),
    ("storyxperts", "vXENYcLe9ibxlo2JVeVBGupkWmL2"),
    ("k5ush5", "dXkGVZ8XN0XE9klt0PAdwd5Ruxg1"),
    ("hEzohEzo", "9ArZj6Vq5PR0KVWHDS7GhkFhSVi1"),
    ("AnubhavGoyal", "VBV9AOCmUHN29YyWu07I9TR7OI43"),
]

# ----------------------------
# CONFIG
# ----------------------------
API_URL = "https://api.manifold.markets/v0/bets"
THRESHOLD = 50
LIMIT = 1000
DELAY = 0.25


# ----------------------------
# FUNCTIONS
# ----------------------------
def quick_check(user_id: str, username, threshold: int = THRESHOLD) -> bool:
    """Return True if user likely has >= threshold bets."""
    resp = requests.get(API_URL, params={"userId": user_id, "limit": threshold + 1})
    if resp.status_code != 200:
        print(f"Error {resp.status_code} for {user_id}")
        return False
    data = resp.json() 
    print(f"[ User Bets {len(data)} vs Theshold {THRESHOLD}] [ Username: {username}]", end=" ")
    return len(data) >= threshold


def fetch_all_bets(user_id: str) -> List[Bet]:
    """Fetch and return all bets for a user as Bet objects."""
    all_bets: List[Bet] = []
    before = None
    while True:
        params = {"userId": user_id, "limit": LIMIT}
        if before:
            params["before"] = before
        resp = requests.get(API_URL, params=params)
        if resp.status_code != 200:
            print(f"Error {resp.status_code} for {user_id}")
            break
        data = resp.json()
        if not data:
            break
        all_bets.extend(Bet.from_payload(b) for b in data)
        if len(data) < LIMIT:
            break
        before = data[-1]["createdTime"]
        time.sleep(DELAY)
    return all_bets


def main():
    qualified_bets: List[Bet] = []

    for username, uid in USERS:
        try:
            if quick_check(uid, username):
                print("qualifies ✅ — fetching full bet history")
                bets = fetch_all_bets(uid)
                qualified_bets.extend(bets)
            else:
                print("below threshold ❌ — skipped")
        except Exception as e:
            print(f"ERROR: {e}")
        time.sleep(DELAY)

    print(f"\nCollected {len(qualified_bets)} total bets from active users.")
    return qualified_bets


if __name__ == "__main__":
    main()