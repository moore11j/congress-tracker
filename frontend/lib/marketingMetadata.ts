import type { Metadata } from "next";

export const WALNUT_MARKETING_URL = "https://walnutmarkets.com";
export const WALNUT_MARKETING_TITLE = "Walnut Markets | Congress Trades & Insider Activity Research";
export const WALNUT_MARKETING_DESCRIPTION =
  "Walnut Markets connects Congress trades, insider activity, government contracts, technicals, fundamentals, institutional filings, and ticker context into confirmation data for serious investors.";
export const WALNUT_OG_DESCRIPTION =
  "The market has tells. Walnut finds them. Research Congress trades, insiders, contracts, technicals, fundamentals, and confirmation scores in one market terminal.";
export const WALNUT_TWITTER_DESCRIPTION =
  "We connect market signals into confirmation data. Built for research. Not investment advice.";
export const WALNUT_SOCIAL_IMAGE_URL = `${WALNUT_MARKETING_URL}/og/walnut-og-v1.png`;
export const WALNUT_SOCIAL_IMAGE_ALT = "Walnut Markets - The market has tells. Walnut finds them.";
export const WALNUT_X_HANDLE = "@Walnutmarkets";
export const WALNUT_X_URL = "https://x.com/Walnutmarkets";
export const WALNUT_REDDIT_URL = "https://www.reddit.com/r/walnutmarkets/";
export const WALNUT_SOCIAL_URLS = [WALNUT_X_URL, WALNUT_REDDIT_URL] as const;

export function marketingCanonicalUrl(pathname: string): string {
  const normalizedPath = pathname === "/" ? "/" : `/${pathname.replace(/^\/+/, "").replace(/\/+$/, "")}`;
  return new URL(normalizedPath, `${WALNUT_MARKETING_URL}/`).toString();
}

export function marketingPageMetadata(pathname: string, metadata: Metadata): Metadata {
  const canonicalUrl = marketingCanonicalUrl(pathname);
  return {
    ...metadata,
    metadataBase: new URL(WALNUT_MARKETING_URL),
    alternates: {
      ...metadata.alternates,
      canonical: canonicalUrl,
    },
    openGraph: {
      ...metadata.openGraph,
      url: canonicalUrl,
    },
  };
}

export function marketingSeoPageMetadata(
  pathname: string,
  {
    title,
    description,
  }: {
    title: string;
    description: string;
  },
): Metadata {
  const canonicalUrl = marketingCanonicalUrl(pathname);
  return marketingPageMetadata(pathname, {
    title,
    description,
    robots: {
      index: true,
      follow: true,
    },
    openGraph: {
      type: "website",
      title,
      description,
      url: canonicalUrl,
      siteName: "Walnut Markets",
      images: [
        {
          url: WALNUT_SOCIAL_IMAGE_URL,
          width: 1200,
          height: 630,
          alt: WALNUT_SOCIAL_IMAGE_ALT,
        },
      ],
    },
    twitter: {
      card: "summary_large_image",
      site: WALNUT_X_HANDLE,
      title,
      description,
      images: [
        {
          url: WALNUT_SOCIAL_IMAGE_URL,
          alt: WALNUT_SOCIAL_IMAGE_ALT,
        },
      ],
    },
  });
}

export const walnutMarketingMetadata: Metadata = {
  metadataBase: new URL(WALNUT_MARKETING_URL),
  title: WALNUT_MARKETING_TITLE,
  description: WALNUT_MARKETING_DESCRIPTION,
  alternates: {
    canonical: marketingCanonicalUrl("/"),
  },
  openGraph: {
    type: "website",
    title: WALNUT_MARKETING_TITLE,
    description: WALNUT_OG_DESCRIPTION,
    url: marketingCanonicalUrl("/"),
    siteName: "Walnut Markets",
    images: [
      {
        url: WALNUT_SOCIAL_IMAGE_URL,
        width: 1200,
        height: 630,
        alt: WALNUT_SOCIAL_IMAGE_ALT,
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    site: WALNUT_X_HANDLE,
    title: WALNUT_MARKETING_TITLE,
    description: WALNUT_TWITTER_DESCRIPTION,
    images: [
      {
        url: WALNUT_SOCIAL_IMAGE_URL,
        alt: WALNUT_SOCIAL_IMAGE_ALT,
      },
    ],
  },
  icons: {
    icon: "/favicon.ico",
    apple: "/apple-touch-icon.png",
  },
};
