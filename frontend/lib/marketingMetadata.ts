import type { Metadata } from "next";
import { homepageContent } from "@/lib/homepageContent";

export const WALNUT_MARKETING_URL = "https://walnutmarkets.com";
export const WALNUT_APP_URL = "https://app.walnutmarkets.com";
export const WALNUT_MARKETING_TITLE = homepageContent.metadata.title;
export const WALNUT_MARKETING_DESCRIPTION = homepageContent.metadata.description;
export const WALNUT_OG_DESCRIPTION = homepageContent.metadata.description;
export const WALNUT_TWITTER_DESCRIPTION = homepageContent.metadata.socialDescription;
export const WALNUT_SOCIAL_IMAGE_URL = `${WALNUT_MARKETING_URL}/og/walnut-og-v1.png`;
export const WALNUT_SOCIAL_IMAGE_ALT = "Walnut Markets stock research and analysis platform.";
export const WALNUT_X_HANDLE = "@Walnutmarkets";
export const WALNUT_X_URL = "https://x.com/Walnutmarkets";
export const WALNUT_REDDIT_URL = "https://www.reddit.com/r/walnutmarkets/";
export const WALNUT_INSTAGRAM_URL = "https://www.instagram.com/walnutmarkets/";
export const WALNUT_TIKTOK_URL = "https://www.tiktok.com/@walnutmarkets";
export const WALNUT_SOCIAL_URLS = [WALNUT_X_URL, WALNUT_REDDIT_URL, WALNUT_INSTAGRAM_URL, WALNUT_TIKTOK_URL] as const;

function metadataText(value: Metadata["title"] | Metadata["description"], fallback: string): string {
  if (typeof value === "string" && value.trim()) return value;
  return fallback;
}

function socialImage() {
  return [
    {
      url: WALNUT_SOCIAL_IMAGE_URL,
      width: 1200,
      height: 630,
      alt: WALNUT_SOCIAL_IMAGE_ALT,
    },
  ];
}

export function marketingCanonicalUrl(pathname: string): string {
  const normalizedPath = pathname === "/" ? "/" : `/${pathname.replace(/^\/+/, "").replace(/\/+$/, "")}`;
  return new URL(normalizedPath, `${WALNUT_MARKETING_URL}/`).toString();
}

export function appCanonicalUrl(pathname: string): string {
  const normalizedPath = pathname === "/" ? "/" : `/${pathname.replace(/^\/+/, "").replace(/\/+$/, "")}`;
  return new URL(normalizedPath, `${WALNUT_APP_URL}/`).toString();
}

export function appPageMetadata(pathname: string, metadata: Metadata): Metadata {
  const canonicalUrl = appCanonicalUrl(pathname);
  const title = metadataText(metadata.title, WALNUT_MARKETING_TITLE);
  const description = metadataText(metadata.description, WALNUT_MARKETING_DESCRIPTION);
  return {
    ...metadata,
    metadataBase: new URL(WALNUT_APP_URL),
    robots: metadata.robots ?? {
      index: true,
      follow: true,
    },
    alternates: {
      ...metadata.alternates,
      canonical: canonicalUrl,
    },
    openGraph: {
      type: "website",
      title,
      description,
      siteName: "Walnut Markets",
      images: socialImage(),
      ...metadata.openGraph,
      url: canonicalUrl,
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
      ...metadata.twitter,
    },
  };
}

export function marketingPageMetadata(pathname: string, metadata: Metadata): Metadata {
  const canonicalUrl = marketingCanonicalUrl(pathname);
  const title = metadataText(metadata.title, WALNUT_MARKETING_TITLE);
  const description = metadataText(metadata.description, WALNUT_MARKETING_DESCRIPTION);
  return {
    ...metadata,
    metadataBase: new URL(WALNUT_MARKETING_URL),
    robots: metadata.robots ?? {
      index: true,
      follow: true,
    },
    alternates: {
      ...metadata.alternates,
      canonical: canonicalUrl,
    },
    openGraph: {
      type: "website",
      title,
      description,
      siteName: "Walnut Markets",
      images: socialImage(),
      ...metadata.openGraph,
      url: canonicalUrl,
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
      ...metadata.twitter,
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
  robots: {
    index: true,
    follow: true,
  },
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
