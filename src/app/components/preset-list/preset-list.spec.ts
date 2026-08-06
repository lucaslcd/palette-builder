import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PresetList } from './preset-list';

describe('PresetList', () => {
  let component: PresetList;
  let fixture: ComponentFixture<PresetList>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PresetList],
    }).compileComponents();

    fixture = TestBed.createComponent(PresetList);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
