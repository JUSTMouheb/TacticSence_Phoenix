import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { HomeComponent } from './components/sections/home/home.component';
import { AboutComponent } from './components/sections/about/about.component';
import { PlayersComponent } from './components/sections/players/players.component';
import { ClubsComponent } from './components/sections/clubs/clubs.component';
//import { AnalyticsComponent } from './components/sections/analytics/analytics.component';
import { ContactComponent } from './components/sections/contact/contact.component';
import { SocialAuthCallbackComponent } from './components/auth/social-auth-callback/social-auth-callback.component';
import { PlayerDashboardComponent } from './dashboards/player-dashboard/player-dashboard.component';
import { AgentDashboardComponent } from './dashboards/agent-dashboard/agent-dashboard.component';
import { ClubStaffDashboardComponent } from './dashboards/club-staff-dashboard/club-staff-dashboard.component';
import { ServiceProviderDashboardComponent } from './dashboards/service-provider-dashboard/service-provider-dashboard.component';
import { AuthGuard } from './guards/auth.guard';
import { RoleGuard } from './guards/role.guard';
const routes: Routes = [
  { path: '', component: HomeComponent },
  { path: 'about', component: AboutComponent },
  { path: 'players', component: PlayersComponent },
  { path: 'clubs', component: ClubsComponent },
  //{ path: 'analytics', component: AnalyticsComponent },
  { path: 'contact', component: ContactComponent },
  { path: '**', redirectTo: '' }, // Add comma here
  {
    path: 'auth/facebook-callback',
    component: SocialAuthCallbackComponent,
    data: { provider: 'facebook' }
  },
  {
    path: 'auth/google-callback',
    component: SocialAuthCallbackComponent,
    data: { provider: 'google' }
  },
  {
    path: 'auth/linkedin-callback',
    component: SocialAuthCallbackComponent,
    data: { provider: 'linkedin' }
  },
  { path: 'player-dashboard', component: PlayerDashboardComponent },
  { path: 'agent-dashboard', component: AgentDashboardComponent },
  { path: 'club-dashboard', component: ClubStaffDashboardComponent },
  { path: 'service-provider-dashboard', component: ServiceProviderDashboardComponent },

];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }