<p align="center">
<h1 align="center"><code>arindas.github.io</code></h1>
</p>

<p align="center">
<a href="https://github.com/arindas/arindas.github.io/actions/workflows/zola-deploy.yml">
<img src="https://github.com/arindas/arindas.github.io/actions/workflows/zola-deploy.yml/badge.svg"/>
</a>
<a href="https://github.com/arindas/arindas.github.io/actions/workflows/pages/pages-build-deployment">
<img src="https://github.com/arindas/arindas.github.io/actions/workflows/pages/pages-build-deployment/badge.svg" />
</a>
</p>

<p align="center">
  <i>arindas'</i> personal website and blog.
</p>

## Local development

The site is built with [Zola](https://www.getzola.org/) and the `tabi` theme,
which is tracked as a Git submodule.

```sh
git clone --recurse-submodules https://github.com/arindas/arindas.github.io.git
cd arindas.github.io
zola check
zola serve
```

Use Zola 0.22.1 or newer. For an existing clone, initialise the theme with
`git submodule update --init --recursive`. Build the production site with
`zola build`; generated files are written to `public/`.

## License

This repository is licensed under the [MIT License](./LICENSE).
