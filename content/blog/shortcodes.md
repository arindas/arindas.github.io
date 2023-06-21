+++
title = "Custom shortcodes"
date = 2023-02-19
updated = 2023-06-05
description = "This theme includes some useful custom shortcodes that you can use to enhance your posts. Whether you want to display images that adapt to light and dark themes, or format a professional-looking reference section, these custom shortcodes have got you covered."

[taxonomies]
tags = ["showcase", "shortcodes"]
+++

## Image shortcodes

### Dual theme images

Useful if you want to use a different image for the light and dark themes:

{{ dual_theme_image(light_src="img/paris_day.webp", dark_src="img/paris_night.webp" alt="The Eiffel tower") }}

#### Usage
```
{{/* dual_theme_image(light_src="img/paris_day.webp", dark_src="img/paris_night.webp" alt="The Eiffel tower") */}}
```

### Invertible image

Good for graphs, line drawings, diagrams… Inverts the colours of the image. The source image will be used for the light theme.

{{ invertible_image(src="img/graph.webp", alt="Invertible graph") }}

#### Usage

```
{{/* invertible_image(src="img/graph.webp", alt="Invertible graph") */}}
```

### Dimmable image

Images with too much brightness or contrast can be jarring against a dark background. Here's an example of a photograph that dims when the dark theme is active.

{{ dimmable_image(src="img/desert_by_oskerwyld.webp", alt="Photograph of a desert, heavenly sky") }}

#### Usage

```
{{/* dimmable_image(src="img/desert_by_oskerwyld.webp", alt="Photograph of a desert, heavenly sky") */}}
```

## Quotes

### Multilingual quotes

This shortcode allows you to display both the translated and original text for a quote:

{{ multilingual_quote(original="Qué sosiego, ir por la vida en silencio, saludando sólo a los amigos.", translated="What tranquility, to go through life in silence, greeting only friends.", author="Francisco Umbral") }}

#### Usage

```
{{/* multilingual_quote(original="Qué sosiego, ir por la vida en silencio, saludando sólo a los amigos.", translated="What tranquility, to go through life in silence, greeting only friends.", author="Francisco Umbral") */}}
```

## References

### Hanging indent

This shortcode formats a reference section with a hanging indent like so:

{% references() %}

Alderson, E. (2015). Cybersecurity and Social Justice: A Critique of Corporate Hegemony in a Digital World. *New York Journal of Technology, 11*(2), 24-39. [https://doi.org/10.1007/s10198-022-01497-6](https://doi.org/10.1007/s10198-022-01497-6).

Funkhouser, M. (2012). The Social Norms of Indecency: An Analysis of Deviant Behavior in Contemporary Society. *Los Angeles Journal of Sociology, 16*(3), 41-58. [https://doi.org/10.1093/jmp/jhx037](https://doi.org/10.1093/jmp/jhx037).

Schrute, D. (2005). The Beet Farming Revolution: An Analysis of Agricultural Innovation. *Scranton Agricultural Quarterly, 38*(3), 67-81.

Steinbrenner, G. (1997). The Cost-Benefit Analysis of George Costanza: An Examination of Risk-Taking Behavior in the Workplace. *New York Journal of Business, 12*(4), 112-125.

Winger, J. A. (2010). The Art of Debate: An Examination of Rhetoric in Greendale Community College's Model United Nations. *Colorado Journal of Communication Studies, 19*(2), 73-86. [https://doi.org/10.1093/6seaons/1movie](https://doi.org/10.1093/6seaons/1movie).

{% end %}

#### Usage

```
{%/* references() */%}

Your references go here.

Each in a new line. Markdown (links, italics…) will be rendered.

{%/* end */%}
```
