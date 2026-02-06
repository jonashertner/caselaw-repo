import "./globals.css";
import type { ReactNode } from "react";
import type { Viewport, Metadata } from "next";
import { Providers } from "./providers";

export const metadata: Metadata = {
  title: "Swiss Case Law AI",
  description: "Search 800k+ Swiss court decisions with AI-powered answers",
  manifest: "/manifest.json",
  appleWebApp: {
    capable: true,
    statusBarStyle: "default",
    title: "Swiss Case Law",
  },
  openGraph: {
    title: "Swiss Case Law AI",
    description: "Search 800k+ Swiss court decisions with AI-powered answers",
    type: "website",
    locale: "de_CH",
    alternateLocale: ["fr_CH", "it_CH", "en"],
  },
  twitter: {
    card: "summary_large_image",
    title: "Swiss Case Law AI",
    description: "Search 800k+ Swiss court decisions with AI-powered answers",
  },
  keywords: [
    "Swiss law",
    "court decisions",
    "case law",
    "BGE",
    "ATF",
    "Bundesgericht",
    "legal research",
    "Rechtsprechung",
    "jurisprudence",
  ],
  authors: [{ name: "Swiss Case Law AI" }],
  creator: "Swiss Case Law AI",
  publisher: "Swiss Case Law AI",
  robots: {
    index: true,
    follow: true,
  },
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
  userScalable: false,
  viewportFit: "cover",
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#ffffff" },
    { media: "(prefers-color-scheme: dark)", color: "#0a0a0a" },
  ],
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="de" suppressHydrationWarning>
      <head>
        <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
        <link rel="apple-touch-icon" href="/favicon.svg" />
        <meta name="msapplication-TileColor" content="#2563eb" />
        <meta name="apple-mobile-web-app-capable" content="yes" />
        <meta name="apple-mobile-web-app-status-bar-style" content="default" />
        <meta name="format-detection" content="telephone=no" />
        <meta name="mobile-web-app-capable" content="yes" />
        {/* Preconnect to API */}
        <link rel="preconnect" href={process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000"} />
        {/* Theme color script to prevent flash */}
        <script
          dangerouslySetInnerHTML={{
            __html: `
              (function() {
                try {
                  var theme = localStorage.getItem('swisslaw-theme');
                  var prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
                  var isDark = theme === 'dark' || (theme !== 'light' && prefersDark);
                  if (isDark) {
                    document.documentElement.setAttribute('data-theme', 'dark');
                  }
                } catch (e) {}
              })();
            `,
          }}
        />
      </head>
      <body>
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
