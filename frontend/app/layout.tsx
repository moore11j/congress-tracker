export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "system-ui", padding: 20 }}>
        <h1>Congress Tracker</h1>
        {children}
      </body>
    </html>
  );
}