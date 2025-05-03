This is a living document and will be treated like a blog to document my progress through this project until I clean up Github for presentation and create a website to publish my findings.

striker-project is a personal project I am working on to learn the basics of data engineering. My main goals are to learn DE fundamentals, GCP, Github, and brush up on my Python and SQL skills.

Why I chose a soccer project
I love my soccer team, Arsenal, but they have one glaring weakness: they lack a goalscorer. Arsenal is the epitome of a team, they spread the goals around among a lot players. But they need someone who can consistently be relied on, whose specialty is scoring goals.

The data I've chosen to use
Soccer is by far the most popular sport on planet Earth, and this presents interesting opportunities from a data analytics standpoint. There is a ton of data out there, which can be both a blessing and a curse. You see, nearly every country on the planet has multiple divisions of professional soccer, which vary drastically in terms of quality, style, pace of play, etc. For example, someone scoring loads of goals in Ligue 1 in France may not do the same in the English Premier League (the league Arsenal plays in) because the English Premier League is much more physical with a quicker pace of play.

There is however a great equalizer, tournaments where teams from different leagues in Europe play each other. The premier competetion with the best teams is called the UEFA Champions League (UCL) and the second-tier competition is called the UEFA Europa League (UEL). I have scraped the past 3 seasons of both competitions to form my data set, inclusive of the current season still progressing. I have included the UEL for 2 reasons: 1. to form a larger dataset 2. sometimes good teams with good players wind up in the Europa League due to league position or dropping down from the Champions League (this is no longer the format, but prior to the 24-25 season, the top teams knocked out of the Champions League tournament were entered into the Europa League tournament).

How I've scraped the data
