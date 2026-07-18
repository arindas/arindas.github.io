(() => {
    const toc = document.querySelector('.toc-container');
    if (!toc) return;

    const links = Array.from(toc.querySelectorAll('a[href*="#"]'));
    const entries = links
        .map((link) => {
            const hash = new URL(link.href, window.location.href).hash;
            const heading = hash ? document.getElementById(decodeURIComponent(hash.slice(1))) : null;
            return heading ? { link, heading } : null;
        })
        .filter(Boolean);

    if (!entries.length) return;

    const setActive = (active) => {
        links.forEach((link) => link.classList.toggle('is-active', link === active.link));
    };

    const updateActiveSection = () => {
        const offset = window.innerHeight * 0.3;
        let active = entries[0];

        for (const entry of entries) {
            if (entry.heading.getBoundingClientRect().top <= offset) active = entry;
            else break;
        }

        setActive(active);
    };

    let ticking = false;
    window.addEventListener('scroll', () => {
        if (ticking) return;
        ticking = true;
        window.requestAnimationFrame(() => {
            updateActiveSection();
            ticking = false;
        });
    }, { passive: true });

    window.addEventListener('hashchange', updateActiveSection);
    updateActiveSection();
})();
