# service_inflation
As the example we have some internet-market with different brands are available for customers in different regions. We want to evaluate the inflation by items, brands, regions and by all service.

We are going to create the datamart for further visualization in some BI tools and firstly we need to clarify the common logic. Let's have a look at the following example.

At some start point (the beginning of the year) some brand in some region sold:
- 5 shirts, each for $500
- 10 pairs of pants, each for $500
- 20 caps, each for $100

At some end point the result was following:
- 8 shirts for $550
- 20 pairs of pants for $510
- 5 caps for $150

Inflation ratio for each item separately is:
- shirts: (550 - 500) / 500 = 0.1
- pants: (510 - 500) / 500 = 0.02
- caps: (150 - 100) / 100 = 0.5

Now we need to define the percantage of total GMV for each item:
- shirts: 550 * 8 / (550 * 8 + 510 * 20 + 150 * 5) = 0.287
- pants: 510 * 20 / (550 * 8 + 510 * 20 + 150 * 5) = 0.665
- caps: 150 * 5 / (550 * 8 + 510 * 20 + 150 * 5) = 0.048

Finally we define **fact inflation** as weighted arithmetic mean:
0.287 * 0.1 + 0.665 * 0.02 + 0.048 * 0.5 = 6.6%

And **perceived inflation** as arithmetic mean:
(0.1 + 0.02 + 0.5) / 3 = 20.7%
