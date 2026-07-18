(() => {
    const toc = document.querySelector('.toc-container');
    if (!toc) return;

    const mobileToggle = document.querySelector('.toc-mobile-toggle');
    const mobileDrawer = document.querySelector('.toc-mobile-drawer');
    const mobileDrawerContent = document.querySelector('.toc-mobile-drawer-content');
    const mobileClose = document.querySelector('.toc-mobile-close');
    if (mobileDrawerContent) {
        const tocList = toc.querySelector('ul');
        if (tocList) mobileDrawerContent.append(tocList.cloneNode(true));
    }

    const mobileQuery = window.matchMedia('(max-width: 1099px)');
    const setTocVisibility = () => {
        toc.open = !mobileQuery.matches;
    };
    setTocVisibility();
    mobileQuery.addEventListener('change', setTocVisibility);

    const links = Array.from(document.querySelectorAll('.toc-container a[href*="#"], .toc-mobile-drawer a[href*="#"]'));
    const currentSections = Array.from(document.querySelectorAll('.toc-current-section'));
    const entries = links
        .map((link) => {
            const hash = new URL(link.href, window.location.href).hash;
            const heading = hash ? document.getElementById(decodeURIComponent(hash.slice(1))) : null;
            return heading ? { link, heading } : null;
        })
        .filter(Boolean);

    if (!entries.length) return;

    const setActive = (active) => {
        links.forEach((link) => link.classList.toggle('is-active', link.hash === active.link.hash));
        currentSections.forEach((section) => {
            section.textContent = active.link.textContent.trim();
        });
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
    const setDrawerOpen = (isOpen) => {
        mobileDrawer?.classList.toggle('is-open', isOpen);
        mobileDrawer?.setAttribute('aria-hidden', String(!isOpen));
        if (mobileDrawer) mobileDrawer.inert = !isOpen;
        mobileToggle?.setAttribute('aria-expanded', String(isOpen));
        document.body.classList.toggle('toc-drawer-open', isOpen);
    };
    mobileToggle?.addEventListener('click', () => setDrawerOpen(true));
    mobileClose?.addEventListener('click', () => setDrawerOpen(false));
    mobileDrawer?.addEventListener('click', (event) => {
        if (event.target.closest('a[href*="#"]')) setDrawerOpen(false);
    });
    mobileQuery.addEventListener('change', () => {
        if (!mobileQuery.matches) setDrawerOpen(false);
    });
    document.addEventListener('click', (event) => {
        if (
            mobileDrawer?.classList.contains('is-open') &&
            !mobileDrawer.contains(event.target) &&
            !mobileToggle?.contains(event.target)
        ) {
            setDrawerOpen(false);
        }
    });

    let touchStart = null;
    document.addEventListener('touchstart', (event) => {
        if (!mobileQuery.matches || event.touches.length !== 1) return;
        const touch = event.touches[0];
        touchStart = {
            x: touch.clientX,
            y: touch.clientY,
            startedInDrawer: Boolean(mobileDrawer?.contains(event.target)),
        };
    }, { passive: true });
    document.addEventListener('touchend', (event) => {
        if (!touchStart || event.changedTouches.length !== 1) return;
        const touch = event.changedTouches[0];
        const deltaX = touch.clientX - touchStart.x;
        const deltaY = Math.abs(touch.clientY - touchStart.y);
        const drawerIsOpen = mobileDrawer?.classList.contains('is-open');

        if (deltaY < 80) {
            if (!drawerIsOpen && touchStart.x > window.innerWidth - 32 && deltaX < -50) {
                setDrawerOpen(true);
            } else if (drawerIsOpen && touchStart.startedInDrawer && deltaX > 50) {
                setDrawerOpen(false);
            }
        }
        touchStart = null;
    }, { passive: true });
    document.addEventListener('touchcancel', () => {
        touchStart = null;
    }, { passive: true });
    window.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') setDrawerOpen(false);
    });
    updateActiveSection();
})();
