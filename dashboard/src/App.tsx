import { Router, Routes, Route, A } from '@solidjs/router';
import AuditList from './routes/AuditList';
import AuditDetail from './routes/AuditDetail';

const App = () => {
  return (
    <Router>
      <div class="app-shell">
        <header class="app-header">
          <A href="/" class="brand">Audit Discovery</A>
          <nav class="nav-links">
            <A href="/" activeClass="active" end>
              Audits
            </A>
          </nav>
        </header>
        <main class="app-main">
          <Routes>
            <Route path="/" component={AuditList} />
            <Route path="/audits/:id" component={AuditDetail} />
          </Routes>
        </main>
      </div>
    </Router>
  );
};

export default App;
