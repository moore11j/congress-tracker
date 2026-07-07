import type { Metadata } from "next";

export const WALNUT_MARKETING_URL = "https://walnutmarkets.com";
export const WALNUT_MARKETING_TITLE = "Walnut Markets | Professional-Grade Market Intelligence";
export const WALNUT_MARKETING_DESCRIPTION =
  "The market has tells. Walnut finds them. Track Congress trades, insider activity, government contracts, signal confirmation, and ticker context in one market terminal.";
export const WALNUT_SOCIAL_IMAGE_URL = `${WALNUT_MARKETING_URL}/og/walnut-og.png`;
export const WALNUT_SOCIAL_IMAGE_ALT = "Walnut Markets \u2014 The market has tells. Walnut finds them.";

export const walnutMarketingMetadata: Metadata = {
  metadataBase: new URL(WALNUT_MARKETING_URL),
  title: WALNUT_MARKETING_TITLE,
  description: WALNUT_MARKETING_DESCRIPTION,
  alternates: {
    canonical: WALNUT_MARKETING_URL,
  },
  openGraph: {
    type: "website",
    title: WALNUT_MARKETING_TITLE,
    description: WALNUT_MARKETING_DESCRIPTION,
    url: WALNUT_MARKETING_URL,
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
    title: WALNUT_MARKETING_TITLE,
    description: WALNUT_MARKETING_DESCRIPTION,
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
