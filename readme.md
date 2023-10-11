# Description
YouBike plays an extremely significant role in Taipei City's transportation system. Many people use YouBike as their primary commuting option, while others use it for transfers, and some even use it as a form of exercise. YouBike truly helps Taipei residents complete the last mile of their transportation needs.

However, at this stage, some people have reported difficulties in either renting or returning bikes. The objective of this project is to address this issue. We analyze historical data to identify unused bicycles and docks, and then reallocate these underutilized resources to high-demand areas to enhance cost-effectiveness. We also use data to explore whether there are alternative stations where users can rent or return bikes if they encounter issues at a particular station.

Furthermore, under the current operational model, YouBike Corporation assign personnel for vehicle dispatch. For users, their primary concern may be whether they can rent or return a bike. For the operators, the challenge is to determine the most effective dispatch method to best meet users' needs. Our analysis aims to identify the most suitable dispatch approach.

In addition to the existing stations, we also give significant consideration to the areas where we plan to set up new stations in the future. We divide Taipei City into 4,309 grids, some of which may already have stations, while others do not. We utilize machine learning to build a model that predicts rental and return counts based on the existing station grids. With this model, we can forecast the potential demand in grids without stations and determine how many docks are needed. Of course, not all grids without stations will require new stations; unsuitable areas will not appear in our recommended station expansion list.

# Acknowledgements
The authors would like to extend our gratitude to **Department of Transportation, Taipei City Government** and **YouBike Corporation** for their contributions and support to this project.
The authors are thankful for the assistance and resources provided, which were instrumental in the successful completion of this project.
