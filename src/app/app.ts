import { Component, inject, signal } from '@angular/core';
import { ColorWheel } from './components/color-wheel/color-wheel';
import { ColorForm } from './components/color-form/color-form';
import { MatTabsModule } from '@angular/material/tabs';
import { MatSelectModule } from '@angular/material/select';
import { form, FormField } from "@angular/forms/signals";
import { ColorStrips } from './components/color-strips/color-strips';
import { MatToolbar } from '@angular/material/toolbar';
import { Header } from "./components/header/header";
import { GlobalState } from './services/global.state';
import { PresetList } from './components/preset-list/preset-list';

@Component({
  selector: 'app-root',
  imports: [ColorWheel, ColorForm, ColorStrips, MatTabsModule, MatSelectModule, FormField, Header, PresetList],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  globalState = inject(GlobalState)

  display = this.globalState.stateForm.display
}
