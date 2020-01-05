# pwills.com

Source for [pwills.com][4], hosted on GitHub Pages. Functions as an online
resume/portfolio, as well as a place for me to rant & rave about technology and
math. 

To build the site locally, do

	git clone https://github.com/peterewills/peterewills.github.io.git
	cd peterewills.github.io
    gem install bundler # to get the dependency manager that jekyll uses
	bundle update # to make sure all ruby gems are up to date
	bundle exec jekyll serve
	
Then navigate to `localhost:4000` in your browser to view the page.

For a more detailed guide on how the site was built, check out [the how-to post
on the blog][5]

Site is built in jekyll on top of [Minimal Mistakes][1]. Uses images from
[Unsplash][2]. If you have questions, drop me a line at [peter@pwills.com](mailto:peter@pwills.com).

[1]: https://github.com/mmistakes/minimal-mistakes

[2]: https://unsplash.com/

[4]: http://www.pwills.com

[5]: http://www.pwills.com/blog/posts/2017/12/20/website.html
